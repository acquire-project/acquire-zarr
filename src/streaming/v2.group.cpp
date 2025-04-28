#include "v2.group.hh"

zarr::V2Group::V2Group(const zarr::GroupConfig& config,
                       std::shared_ptr<ThreadPool> thread_pool)
  : Group(config, thread_pool)
{
}

zarr::V2Group::V2Group(const zarr::GroupConfig& config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Group(config, thread_pool, s3_connection_pool)
{
}

nlohmann::json
zarr::V2Group::get_ome_metadata() const
{
    auto multiscales = make_multiscales_metadata_();
    multiscales[0]["version"] = "0.4";
    multiscales[0]["name"] = "/";
    return multiscales;
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
        const auto config = make_array_config_();
        arrays_.push_back(std::make_unique<zarr::V2Array>(
          config, thread_pool_, s3_connection_pool_));
    }

    return true;
}
