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

bool
zarr::ArrayBase::write_custom_metadata(const std::string& key,
                                       const nlohmann::json& metadata)
{
    const std::vector<std::string> reserved_keys{ "ome" };

    // write immediately under 'attributes'
    if (key.empty()) {
        // check every reserved key to avoid collisions with custom metadata
        // keys
        for (const auto& reserved_key : reserved_keys) {
            if (metadata.contains(reserved_key)) {
                LOG_ERROR("Key '", key, "' is reserved and cannot be used");
                return false;
            }
        }

        for (const auto& [key, value] : metadata.items()) {
            custom_metadata_[key] = value;
        }
    } else {
        if (const auto it =
              std::find(reserved_keys.begin(), reserved_keys.end(), key);
            it != reserved_keys.end()) {
            LOG_ERROR("Key '", key, "' is reserved and cannot be used.");
            return false;
        }

        // store custom metadata in memory until we write it out in close()
        custom_metadata_[key] = metadata;
    }

    return true;
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
zarr::ArrayBase::make_metadata_sink_()
{
    metadata_sinks_.clear();

    try {
        const std::string path = node_path_() + "/" + metadata_path_;
        auto sink =
          config_->bucket_name
            ? make_s3_sink(*config_->bucket_name, path, s3_connection_pool_)
            : make_file_sink(path, file_handle_pool_);
        if (!sink) {
            LOG_ERROR("Failed to create metadata sink for path: ", path);
            return false;
        }
        metadata_sink_ = std::move(sink);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to create metadata sinks: ", exc.what());
        return false;
    }

    return metadata_sink_ != nullptr;
}

bool
zarr::ArrayBase::write_metadata_()
{
    nlohmann::json metadata;
    if (!make_metadata_(metadata)) {
        LOG_ERROR("Failed to make metadata.");
        return false;
    }

    if (!make_metadata_sink_()) {
        LOG_ERROR("Failed to make metadata sinks.");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    std::span data{ reinterpret_cast<const uint8_t*>(metadata_str.data()),
                    metadata_str.size() };

    if (!metadata_sink_->write(0, data)) {
        LOG_ERROR("Failed to write metadata for key: ", metadata_path_);
        return false;
    }

    return true;
}

std::unique_ptr<zarr::ArrayBase>
zarr::make_array(std::shared_ptr<ArrayConfig> config,
                 std::shared_ptr<ThreadPool> thread_pool,
                 std::shared_ptr<FileHandlePool> file_handle_pool,
                 std::shared_ptr<S3ConnectionPool> s3_connection_pool,
                 bool is_hcs_array)
{
    const auto multiscale = config->downsampling_method.has_value();

    std::unique_ptr<ArrayBase> array;
    if (multiscale || is_hcs_array) {
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
