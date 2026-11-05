#include <utility>

#include "array.hh"
#include "array.base.hh"
#include "macros.hh"
#include "multiscale.array.hh"

zarr::ArrayBase::ArrayBase(std::shared_ptr<ArrayConfig> config,
                           std::shared_ptr<ThreadPool> thread_pool,
                           std::shared_ptr<FileHandlePool> file_handle_pool,
                           std::shared_ptr<S3Client> s3_client)
  : config_(config)
  , thread_pool_(thread_pool)
  , s3_client_(s3_client)
  , file_handle_pool_(file_handle_pool)
{
    CHECK(config_);      // required
    CHECK(thread_pool_); // required
    EXPECT(s3_client_ != nullptr || file_handle_pool_ != nullptr,
           "Either an S3 client or a file handle pool must be provided.");
}

namespace {
const std::string ome_key = "ome";

bool
check_custom_ome(const nlohmann::json& ome, const nlohmann::json& library_ome)
{
    if (!ome.is_object()) {
        LOG_ERROR("Custom '", ome_key, "' metadata must be a JSON object");
        return false;
    }

    if (ome.contains("multiscales")) {
        LOG_ERROR(
          "Key '", ome_key, "/multiscales' is reserved: the stream writes it");
        return false;
    }

    if (!library_ome.is_object()) {
        return true;
    }

    for (const auto& [key, value] : ome.items()) {
        if (!library_ome.contains(key)) {
            continue;
        }

        // the stream's version describes the keys it writes
        if (key == "version" && value == library_ome[key]) {
            continue;
        }

        LOG_ERROR("Key '",
                  ome_key,
                  "/",
                  key,
                  "' is written by the stream on this node, as ",
                  library_ome[key].dump());
        return false;
    }

    return true;
}
} // namespace

bool
zarr::merge_custom_metadata(
  std::unordered_map<std::string, nlohmann::json>& attributes,
  const std::string& key,
  const nlohmann::json& metadata,
  const nlohmann::json& library_ome)
{
    // write immediately under 'attributes'
    if (key.empty()) {
        if (metadata.is_object() && metadata.contains(ome_key) &&
            !check_custom_ome(metadata[ome_key], library_ome)) {
            return false;
        }

        for (const auto& [k, value] : metadata.items()) {
            attributes[k] = value;
        }
    } else {
        if (key == ome_key) {
            if (!check_custom_ome(metadata, library_ome)) {
                return false;
            }
        } else if (key.starts_with(ome_key + "/")) {
            LOG_ERROR("Key '",
                      key,
                      "' is not allowed: a metadata key is not a path. Write "
                      "an object with the metadata key '",
                      ome_key,
                      "'.");
            return false;
        }

        attributes[key] = metadata;
    }

    return true;
}

void
zarr::apply_custom_metadata(
  nlohmann::json& attributes,
  const std::unordered_map<std::string, nlohmann::json>& custom_metadata)
{
    for (const auto& [key, value] : custom_metadata) {
        if (key == ome_key && value.is_object() &&
            attributes.contains(ome_key) && attributes[ome_key].is_object()) {
            attributes[ome_key].update(value);
        } else {
            attributes[key] = value;
        }
    }
}

bool
zarr::ArrayBase::write_custom_metadata(const std::string& key,
                                       const nlohmann::json& metadata)
{
    // store custom metadata in memory until we write it out in close()
    std::unique_lock lock(metadata_mutex_);
    return merge_custom_metadata(
      custom_metadata_, key, metadata, library_ome_metadata_());
}

nlohmann::json
zarr::ArrayBase::library_ome_metadata_() const
{
    return nullptr;
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
    try {
        const std::string path = node_path_() + "/" + metadata_path_;
        auto sink =
          config_->bucket_name
            ? make_s3_sink(*config_->bucket_name, path, s3_client_)
            : make_file_sink(path, file_handle_pool_, /*truncate_to_fit=*/true);
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
