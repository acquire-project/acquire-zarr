#include "macros.hh"
#include "v3.group.hh"
#include "zarr.common.hh"

zarr::V3Group::V3Group(std::shared_ptr<GroupConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Group(std::move(config),
          std::move(thread_pool),
          std::move(s3_connection_pool))
{
    // dimensions may be null in the case of intermediate groups, e.g., the
    // A in A/1
    if (config_->dimensions) {
        CHECK(create_arrays_());
    }
}

std::string
zarr::V3Group::get_metadata_key() const
{
    std::string key = config_->store_root;
    if (!config_->group_key.empty()) {
        key += "/" + config_->group_key;
    }
    key += "/zarr.json";

    return key;
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
        const auto config = make_base_array_config_();
        arrays_.push_back(std::make_unique<zarr::V3Array>(
          config, thread_pool_, s3_connection_pool_));
    }

    return true;
}

nlohmann::json
zarr::V3Group::make_group_metadata_() const
{
    nlohmann::json metadata = {
        { "zarr_format", 3 },
        { "consolidated_metadata", nullptr },
        { "node_type", "group" },
        { "attributes", nlohmann::json::object() },
    };

    if (!arrays_.empty()) {
        metadata["attributes"]["ome"] = get_ome_metadata();
    }

    return metadata;
}