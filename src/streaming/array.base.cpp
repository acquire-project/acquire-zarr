#include <utility>

#include "array.base.hh"
#include "multiscale.array.hh"
#include "macros.hh"
#include "v2.array.hh"
#include "v3.array.hh"
#include "v2.multiscale.array.hh"
#include "v3.multiscale.array.hh"

zarr::ArrayBase::ArrayBase(std::shared_ptr<ArrayConfig> config,
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
zarr::ArrayBase::node_path_() const
{
    std::string key = config_->store_root;
    if (!config_->node_key.empty()) {
        key += "/" + config_->node_key;
    }

    return key;
}

bool
zarr::ArrayBase::make_metadata_sinks_()
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

            if (sink == nullptr) {
                LOG_ERROR("Failed to create metadata sink for ", key);
                return false;
            }
            metadata_sinks_.emplace(key, std::move(sink));
        }
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to create metadata sinks: ", exc.what());
        return false;
    }

    return true;
}

bool
zarr::ArrayBase::write_metadata_()
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

        std::span data{ reinterpret_cast<const uint8_t*>(metadata.data()),
                        metadata.size() };
        if (!sink->write(0, data)) {
            LOG_ERROR("Failed to write metadata for key: ", key);
            return false;
        }
    }

    return true;
}

std::unique_ptr<zarr::ArrayBase>
zarr::make_array(std::shared_ptr<zarr::ArrayConfig> config,
                 std::shared_ptr<ThreadPool> thread_pool,
                 std::shared_ptr<S3ConnectionPool> s3_connection_pool,
                 ZarrVersion format)
{
    // create a multiscale array at the dataset root (node_key is empty) or if
    // we have a genuine multiscale dataset
    const auto multiscale =
      config->node_key.empty() || config->downsampling_method.has_value();
    EXPECT(format < ZarrVersionCount,
           "Invalid Zarr format: ",
           static_cast<int>(format));

    std::unique_ptr<ArrayBase> array;
    if (multiscale) {
        if (format == ZarrVersion_2) {
            array = std::make_unique<V2MultiscaleArray>(
              config, thread_pool, s3_connection_pool);
        } else {
            array = std::make_unique<V3MultiscaleArray>(
              config, thread_pool, s3_connection_pool);
        }
    } else {
        if (format == ZarrVersion_2) {
            array = std::make_unique<V2Array>(
              config, thread_pool, s3_connection_pool);
        } else {
            array = std::make_unique<V3Array>(
              config, thread_pool, s3_connection_pool);
        }
    }

    return array;
}

bool
zarr::finalize_node(std::unique_ptr<ArrayBase>&& node)
{
    if (!node) {
        LOG_INFO("Node is null, nothing to finalize.");
        return true;
    }

    if (auto ms_array = downcast_node<MultiscaleArray>(std::move(node))) {
        if (!finalize_group(std::move(ms_array))) {
            LOG_ERROR("Failed to finalize multiscale array.");
            node.reset(ms_array.release());
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
