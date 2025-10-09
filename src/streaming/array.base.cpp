#include <utility>

#include "array.hh"
#include "array.base.hh"
#include "macros.hh"
#include "multiscale.array.hh"

zarr::ArrayBase::ArrayBase(std::shared_ptr<ArrayConfig> config,
                           std::shared_ptr<ThreadPool> thread_pool,
                           std::shared_ptr<FileHandlePool> file_handle_pool,
                           std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_(config)
  , thread_pool_(thread_pool)
  , s3_connection_pool_(s3_connection_pool)
  , file_handle_pool_(file_handle_pool)
{
    CHECK(config_);      // required
    CHECK(thread_pool_); // required
    EXPECT(s3_connection_pool_ != nullptr || file_handle_pool_ != nullptr,
           "Either S3 connection pool or file handle pool must be provided.");
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
                : make_file_sink(path, file_handle_pool_);

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
zarr::make_array(std::shared_ptr<ArrayConfig> config,
                 std::shared_ptr<ThreadPool> thread_pool,
                 std::shared_ptr<FileHandlePool> file_handle_pool,
                 std::shared_ptr<S3ConnectionPool> s3_connection_pool)
{
    // create a multiscale array at the dataset root (node_key is empty) or if
    // we have a genuine multiscale dataset
    const auto multiscale =
      config->node_key.empty() || config->downsampling_method.has_value();

    std::unique_ptr<ArrayBase> array;
    if (multiscale) {
        array = std::make_unique<MultiscaleArray>(
          config, thread_pool, file_handle_pool, s3_connection_pool);
    } else {
        array = std::make_unique<Array>(
          config, thread_pool, file_handle_pool, s3_connection_pool);
    }

    return array;
}

bool
zarr::finalize_array(std::unique_ptr<ArrayBase>&& array)
{
    if (array == nullptr) {
        LOG_INFO("Array is null. Nothing to finalize.");
        return true;
    }

    try {
        bool result = array->close_();
        return result;
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to close array: ", exc.what());
        return false;
    }
}
