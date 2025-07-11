#include "macros.hh"
#include "multiscale.array.hh"
#include "zarr.common.hh"

namespace {
std::string
dimension_type_to_string(ZarrDimensionType type)
{
    switch (type) {
        case ZarrDimensionType_Time:
            return "time";
        case ZarrDimensionType_Channel:
            return "channel";
        case ZarrDimensionType_Space:
            return "space";
        case ZarrDimensionType_Other:
            return "other";
        default:
            return "(unknown)";
    }
}
} // namespace

zarr::MultiscaleArray::MultiscaleArray(
  std::shared_ptr<ArrayConfig> config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayBase(config, thread_pool, s3_connection_pool)
{
    bytes_per_frame_ =
      config_->dimensions == nullptr
        ? 0
        : zarr::bytes_of_frame(*config_->dimensions, config_->dtype);

    EXPECT(create_downsampler_(), "Failed to create downsampler");
}

size_t
zarr::MultiscaleArray::write_frame(LockedBuffer& data)
{
    if (arrays_.empty()) {
        LOG_WARNING("Attempt to write to group with no arrays");
        return 0;
    }

    const auto n_bytes = arrays_[0]->write_frame(data);
    EXPECT(n_bytes == bytes_per_frame_,
           "Expected to write ",
           bytes_per_frame_,
           " bytes, wrote ",
           n_bytes);

    if (n_bytes != data.size()) {
        LOG_ERROR("Incomplete write to full-resolution array");
        return n_bytes;
    }

    write_multiscale_frames_(data);
    return n_bytes;
}

bool
zarr::MultiscaleArray::close_()
{
    for (auto i = 0; i < arrays_.size(); ++i) {
        if (!finalize_array(std::move(arrays_[i]))) {
            LOG_ERROR("Error closing group: failed to finalize array ", i);
            return false;
        }
    }

    if (!write_metadata_()) {
        LOG_ERROR("Error closing group: failed to write metadata");
        return false;
    }

    for (auto& [key, sink] : metadata_sinks_) {
        EXPECT(zarr::finalize_sink(std::move(sink)),
               "Failed to finalize metadata sink ",
               key);
    }

    arrays_.clear();
    metadata_sinks_.clear();

    return true;
}

bool
zarr::MultiscaleArray::create_downsampler_()
{
    if (!config_->downsampling_method) {
        return true; // no downsampling method specified, nothing to do
    }

    const auto config = make_base_array_config_();

    try {
        downsampler_ =
          std::make_unique<Downsampler>(config, *config_->downsampling_method);
    } catch (const std::exception& exc) {
        LOG_ERROR("Error creating downsampler: " + std::string(exc.what()));
    }

    return downsampler_ != nullptr;
}

nlohmann::json
zarr::MultiscaleArray::make_multiscales_metadata_() const
{
    nlohmann::json multiscales;
    const auto ndims = config_->dimensions->ndims();

    auto& axes = multiscales[0]["axes"];
    for (auto i = 0; i < ndims; ++i) {
        const auto& dim = config_->dimensions->at(i);
        const auto type = dimension_type_to_string(dim.type);
        const std::string unit = dim.unit.has_value() ? *dim.unit : "";

        if (!unit.empty()) {
            axes.push_back({
              { "name", dim.name.c_str() },
              { "type", type },
              { "unit", unit.c_str() },
            });
        } else {
            axes.push_back({ { "name", dim.name.c_str() }, { "type", type } });
        }
    }

    // spatial multiscale metadata
    std::vector<double> scales(ndims);
    for (auto i = 0; i < ndims; ++i) {
        const auto& dim = config_->dimensions->at(i);
        scales[i] = dim.scale;
    }

    multiscales[0]["datasets"] = {
        {
          { "path", "0" },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        },
    };

    const auto& base_config = make_base_array_config_();
    const auto& base_dims = base_config->dimensions;

    for (auto i = 1; i < arrays_.size(); ++i) {
        const auto& config = downsampler_->writer_configurations().at(i);

        for (auto j = 0; j < ndims; ++j) {
            const auto& base_dim = base_dims->at(j);
            const auto& down_dim = config->dimensions->at(j);
            if (base_dim.type != ZarrDimensionType_Space) {
                continue;
            }

            const auto base_size = base_dim.array_size_px;
            const auto down_size = down_dim.array_size_px;
            const auto ratio = (base_size + down_size - 1) / down_size;

            // scale by next power of 2
            scales[j] = base_dim.scale * std::bit_ceil(ratio);
        }

        multiscales[0]["datasets"].push_back({
          { "path", std::to_string(i) },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        });

        // downsampling metadata
        multiscales[0]["type"] = downsampler_->downsampling_method();
        multiscales[0]["metadata"] = downsampler_->get_metadata();
    }

    return multiscales;
}

std::shared_ptr<zarr::ArrayConfig>
zarr::MultiscaleArray::make_base_array_config_() const
{
    return std::make_shared<ArrayConfig>(config_->store_root,
                                         config_->node_key + "/0",
                                         config_->bucket_name,
                                         config_->compression_params,
                                         config_->dimensions,
                                         config_->dtype,
                                         std::nullopt,
                                         0);
}

void
zarr::MultiscaleArray::write_multiscale_frames_(LockedBuffer& data)
{
    if (!downsampler_) {
        return; // no downsampler, nothing to do
    }

    downsampler_->add_frame(data);

    for (auto i = 1; i < arrays_.size(); ++i) {
        LockedBuffer downsampled_frame;
        if (downsampler_->take_frame(i, downsampled_frame)) {
            const auto n_bytes = arrays_[i]->write_frame(downsampled_frame);
            EXPECT(n_bytes == downsampled_frame.size(),
                   "Expected to write ",
                   downsampled_frame.size(),
                   " bytes to multiscale array ",
                   i,
                   "wrote ",
                   n_bytes);
        }
    }
}

bool
zarr::finalize_group(std::unique_ptr<MultiscaleArray>&& array)
{
    if (array == nullptr) {
        LOG_INFO("MultiscaleArray is null. Nothing to finalize.");
        return true;
    }

    try {
        if (!array->close_()) {
            return false;
        }
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to close multiscale array: ", exc.what());
        return false;
    }

    array.reset();
    return true;
}
