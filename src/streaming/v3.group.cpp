#include "macros.hh"
#include "v3.group.hh"

zarr::V3Group::V3Group(const zarr::GroupConfig& config,
                       std::shared_ptr<ThreadPool> thread_pool)
  : V3Group(config, thread_pool, nullptr)
{
}

zarr::V3Group::V3Group(const zarr::GroupConfig& config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Group(config, thread_pool, s3_connection_pool)
{
    CHECK(create_arrays_());
}

nlohmann::json
zarr::V3Group::get_ome_metadata() const
{
    nlohmann::json ome;
    ome["version"] = "0.5";
    ome["name"] = "/";
    ome["multiscales"] = make_multiscales_metadata_();

    return ome;
}

bool
zarr::V3Group::create_arrays_()
{
    arrays_.clear();

    if (downsampler_) {
        const auto& configs = downsampler_->writer_configurations();
        arrays_.resize(configs.size());

        for (const auto& [lod, config] : configs) {
            arrays_[lod] = std::make_unique<zarr::V3Array>(
              config, thread_pool_, s3_connection_pool_);
        }
    } else {
        const auto config = make_array_config_();
        arrays_.push_back(std::make_unique<zarr::V3Array>(
          config, thread_pool_, s3_connection_pool_));
    }

    return true;
}