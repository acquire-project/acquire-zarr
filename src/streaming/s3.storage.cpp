#include "macros.hh"
#include "s3.storage.hh"

#include <acquire.zarr.h>

zarr::S3Storage::S3Storage(const std::string& bucket_name,
                           std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : bucket_name_(bucket_name)
  , s3_connection_pool_(std::move(s3_connection_pool))
{
    EXPECT(!bucket_name_.empty(), "S3 bucket name is empty");
    EXPECT(s3_connection_pool_, "S3 connection pool is null");
}

bool
zarr::S3Storage::finalize_object(const std::string& path)
{
    if (const auto it = s3_objects_.find(path); it != s3_objects_.end()) {
        if (const auto& s3_object = it->second; s3_object != nullptr) {
            if (!s3_object->close()) {
                LOG_ERROR("Failed to finalize S3 object at ", path);
                return false;
            }
        }
        s3_objects_.erase(it);

        return true;
    }

    return false;
}

void
zarr::S3Storage::create_s3_object_(const std::string& key)
{
    if (!s3_objects_.contains(key)) {
        s3_objects_.emplace(
          key,
          std::make_unique<S3Object>(bucket_name_, key, s3_connection_pool_));
    }
}

bool
zarr::S3Storage::write_binary(const std::string& key,
                              const std::vector<uint8_t>& data,
                              size_t offset)
{
    create_s3_object_(key);

    auto it = s3_objects_.find(key);
    EXPECT(it != s3_objects_.end(), "S3 object at ", key, " not found");
    if (auto& s3_object = it->second; s3_object != nullptr) {
        return s3_object->write(data, offset);
    }

    LOG_ERROR("S3 object at ", key, " is null");
    return false;
}

bool
zarr::S3Storage::write_string(const std::string& key,
                              const std::string& data,
                              size_t offset)
{
    create_s3_object_(key);

    auto it = s3_objects_.find(key);
    EXPECT(it != s3_objects_.end(), "S3 object at ", key, " not found");
    if (auto& s3_object = it->second; s3_object != nullptr) {
        std::span span{ reinterpret_cast<const uint8_t*>(data.data()),
                        data.size() };
        return s3_object->write(span, offset);
    }

    LOG_ERROR("S3 object at ", key, " is null");
    return false;
}
