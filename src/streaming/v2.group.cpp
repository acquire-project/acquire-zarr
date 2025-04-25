#include "v2.group.hh"

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
        const auto config = make_array_config_();
        arrays_.push_back(std::make_unique<zarr::V2Array>(
          config, thread_pool_, s3_connection_pool_));
    }

    return true;
}

nlohmann::json
zarr::V2Group::make_ome_metadata_() const
{
    auto multiscales = make_multiscales_metadata_();
    multiscales[0]["version"] = "0.4";
    multiscales[0]["name"] = "/";
    return multiscales;
}
