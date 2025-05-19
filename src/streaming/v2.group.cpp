#include "macros.hh"
#include "v2.group.hh"
#include "zarr.common.hh"

zarr::V2Group::V2Group(std::shared_ptr<GroupConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Group(config,
          thread_pool,
          s3_connection_pool)
{
    // dimensions may be null in the case of intermediate groups, e.g., the
    // A in A/1
    if (config_->dimensions) {
        CHECK(create_arrays_());
    }
}

std::vector<std::string>
zarr::V2Group::metadata_keys_() const
{
    return { ".zattrs", ".zgroup" };
}

bool
zarr::V2Group::make_metadata_()
{
    metadata_strings_.clear();

    nlohmann::json metadata;

    // .zattrs
    metadata = { { "multiscales", get_ome_metadata_() } };
    metadata_strings_.emplace(".zattrs", metadata.dump(4));

    // .zgroup
    metadata = { { "zarr_format", 2 } };
    metadata_strings_.emplace(".zgroup", metadata.dump(4));

    return true;
}

bool
zarr::V2Group::create_arrays_()
{
    arrays_.clear();

    if (downsampler_) {
        const auto& configs = downsampler_->writer_configurations();
        arrays_.resize(configs.size());

        for (const auto& [lod, config] : configs) {
            arrays_[lod] = std::make_unique<zarr::V2Array>(
              config, thread_pool_, s3_connection_pool_);
        }
    } else {
        const auto config = make_base_array_config_();
        arrays_.push_back(std::make_unique<zarr::V2Array>(
          config, thread_pool_, s3_connection_pool_));
    }

    return true;
}

nlohmann::json
zarr::V2Group::get_ome_metadata_() const
{
    auto multiscales = make_multiscales_metadata_();
    multiscales[0]["version"] = "0.4";
    multiscales[0]["name"] = "/";
    return multiscales;
}
