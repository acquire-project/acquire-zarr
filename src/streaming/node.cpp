#include <utility>

#include "array.hh"
#include "group.hh"
#include "macros.hh"
#include "node.hh"

zarr::ZarrNode::ZarrNode(std::shared_ptr<ZarrNodeConfig> config,
                         std::shared_ptr<ThreadPool> thread_pool,
                         std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_(config)
  , thread_pool_(thread_pool)
  , s3_connection_pool_(s3_connection_pool)
{
    CHECK(config_);      // required
    CHECK(thread_pool_); // required
}

std::string
zarr::ZarrNode::node_path_() const
{
    std::string key = config_->store_root;
    if (!config_->node_key.empty()) {
        key += "/" + config_->node_key;
    }

    return key;
}

bool
zarr::ZarrNode::make_metadata_sinks_()
{
    metadata_sinks_.clear();

    try {
        const auto sink_keys = metadata_keys_();
        for (const auto& key : sink_keys) {
            const std::string path = node_path_() + "/" + key;
            std::unique_ptr<Sink> sink =
              config_->bucket_name
                ? make_s3_sink(*config_->bucket_name, path, s3_connection_pool_)
                : make_file_sink(path);

            metadata_sinks_.emplace(key, std::move(sink));
        }
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to create metadata sinks: ", exc.what());
        return false;
    }

    return true;
}

bool
zarr::ZarrNode::write_metadata_()
{
    if (!make_metadata_()) {
        LOG_ERROR("Failed to make metadata.");
        return false;
    }

    if (!make_metadata_sinks_()) {
        LOG_ERROR("Failed to make metadata sinks.");
        return false;
    }

    for (const auto& [key, metadata] : metadata_strings_) {
        const auto it = metadata_sinks_.find(key);
        if (it == metadata_sinks_.end()) {
            LOG_ERROR("Metadata sink not found for key: ", key);
            return false;
        }

        auto& sink = it->second;
        if (!sink) {
            LOG_ERROR("Metadata sink is null for key: ", key);
            return false;
        }

        std::span data{ reinterpret_cast<const std::byte*>(metadata.data()),
                        metadata.size() };
        if (!sink->write(0, data)) {
            LOG_ERROR("Failed to write metadata for key: ", key);
            return false;
        }
    }

    return true;
}

bool
zarr::finalize_node(std::unique_ptr<ZarrNode>&& node)
{
    if (!node) {
        LOG_INFO("Node is null, nothing to finalize.");
        return true;
    }

    if (auto group = downcast_node<Group>(std::move(node))) {
        if (!finalize_group(std::move(group))) {
            LOG_ERROR("Failed to finalize group.");
            node.reset(group.release());
            return false;
        }
    } else if (auto array = downcast_node<Array>(std::move(node))) {
        if (!finalize_array(std::move(array))) {
            LOG_ERROR("Failed to finalize array.");
            node.reset(array.release());
            return false;
        }
    } else {
        LOG_ERROR("Unknown node type.");
        return false;
    }

    node.reset();
    return true;
}
