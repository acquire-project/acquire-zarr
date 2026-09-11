#pragma once

/// Shared S3 helpers for the tests. Reads the endpoint/bucket/region from the
/// environment and verifies what was written, so each test does not carry its
/// own copy.

#include "logger.hh"
#include "s3.client.hh"

#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace test {
/**
 * @brief Read S3 settings from the environment.
 * @returns The settings, or nullopt if the environment is not configured, so
 * callers can skip rather than fail.
 */
inline std::optional<zarr::S3Settings>
s3_settings_from_env()
{
    zarr::S3Settings settings;

    const char* env = std::getenv("ZARR_S3_ENDPOINT");
    if (!env) {
        LOG_WARNING("ZARR_S3_ENDPOINT not set.");
        return std::nullopt;
    }
    settings.endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_WARNING("ZARR_S3_BUCKET_NAME not set.");
        return std::nullopt;
    }
    settings.bucket_name = env;

    if ((env = std::getenv("ZARR_S3_REGION"))) {
        settings.region = env;
    }

    return settings;
}

/**
 * @brief Construct a client from the environment.
 * @returns The client, or nullptr if the environment is not configured.
 */
inline std::unique_ptr<zarr::S3Client>
s3_client_from_env()
{
    auto settings = s3_settings_from_env();
    if (!settings) {
        return nullptr;
    }

    return std::make_unique<zarr::S3Client>(*settings);
}
} // namespace test

inline bool
object_exists(zarr::S3Client& client,
              const std::string& bucket_name,
              const std::string& object_name)
{
    return client.object_exists(bucket_name, object_name);
}

inline size_t
get_object_size(zarr::S3Client& client,
                const std::string& bucket_name,
                const std::string& object_name)
{
    const auto size = client.object_size(bucket_name, object_name);
    if (!size) {
        LOG_ERROR("Failed to get object size: ", object_name);
        return 0;
    }

    return *size;
}

inline std::string
get_object_contents(zarr::S3Client& client,
                    const std::string& bucket_name,
                    const std::string& object_name)
{
    const auto contents = client.get_object(bucket_name, object_name);
    if (!contents) {
        LOG_ERROR("Failed to get object contents: ", object_name);
        return {};
    }

    return { contents->begin(), contents->end() };
}

inline bool
remove_items(zarr::S3Client& client,
             const std::string& bucket_name,
             const std::vector<std::string>& object_names)
{
    bool all_removed = true;
    for (const auto& object_name : object_names) {
        if (!client.delete_object(bucket_name, object_name)) {
            LOG_ERROR("Failed to delete object ", object_name);
            all_removed = false;
        }
    }

    return all_removed;
}
