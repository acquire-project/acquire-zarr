#include "sink.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "macros.hh"

#include <algorithm>
#include <filesystem>
#include <future>
#include <stdexcept>
#include <unordered_set>

namespace fs = std::filesystem;

namespace {
bool
bucket_exists(std::string_view bucket_name,
              std::shared_ptr<zarr::S3ConnectionPool> connection_pool)
{
    CHECK(!bucket_name.empty());
    EXPECT(connection_pool, "S3 connection pool not provided.");

    auto conn = connection_pool->get_connection();
    bool bucket_exists = conn->bucket_exists(bucket_name);

    connection_pool->return_connection(std::move(conn));

    return bucket_exists;
}

bool
make_s3_sinks(std::string_view bucket_name,
              const std::vector<std::string>& object_keys,
              std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
              std::vector<std::unique_ptr<zarr::Sink>>& sinks)
{
    if (object_keys.empty()) {
        return true;
    }

    if (bucket_name.empty()) {
        LOG_ERROR("Bucket name not provided.");
        return false;
    }
    if (!connection_pool) {
        LOG_ERROR("S3 connection pool not provided.");
        return false;
    }

    const auto n_objects = object_keys.size();
    sinks.resize(n_objects);
    for (auto i = 0; i < n_objects; ++i) {
        sinks[i] = std::make_unique<zarr::S3Object>(
          bucket_name, object_keys[i], connection_pool);
    }

    return true;
}
} // namespace

bool
zarr::finalize_sink(std::unique_ptr<zarr::Sink>&& sink)
{
    if (sink == nullptr) {
        LOG_INFO("Sink is null. Nothing to finalize.");
        return true;
    }

    if (!sink->flush_()) {
        return false;
    }

    sink.reset();
    return true;
}

std::vector<std::string>
zarr::construct_data_paths(std::string_view base_path,
                           const ArrayDimensions& dimensions,
                           const DimensionPartsFun& parts_along_dimension)
{


    return paths_out;
}

std::unique_ptr<zarr::Sink>
zarr::make_file_sink(std::string_view file_path,
                     std::shared_ptr<FileHandlePool> file_handle_pool)
{
    if (file_path.starts_with("file://")) {
        file_path = file_path.substr(7);
    }

    EXPECT(!file_path.empty(), "File path must not be empty.");

    fs::path path(file_path);
    EXPECT(!path.empty(), "Invalid file path: ", file_path);

    fs::path parent_path = path.parent_path();

    if (!fs::is_directory(parent_path)) {
        std::error_code ec;
        if (!fs::create_directories(parent_path, ec) &&
            !fs::is_directory(parent_path)) {
            LOG_ERROR(
              "Failed to create directory '", parent_path, "': ", ec.message());
            return nullptr;
        }
    }

    return std::make_unique<FileSink>(file_path, file_handle_pool);
}

std::unique_ptr<zarr::Sink>
zarr::make_s3_sink(std::string_view bucket_name,
                   std::string_view object_key,
                   std::shared_ptr<S3ConnectionPool> connection_pool)
{
    EXPECT(!object_key.empty(), "Object key must not be empty.");

    // bucket name and connection pool are checked in bucket_exists
    if (!bucket_exists(bucket_name, connection_pool)) {
        LOG_ERROR("Bucket '", bucket_name, "' does not exist.");
        return nullptr;
    }

    return std::make_unique<S3Object>(bucket_name, object_key, connection_pool);
}

bool
zarr::make_data_s3_sinks(std::string_view bucket_name,
                         std::string_view base_path,
                         const ArrayDimensions& dimensions,
                         const DimensionPartsFun& parts_along_dimension,
                         std::shared_ptr<S3ConnectionPool> connection_pool,
                         std::vector<std::unique_ptr<Sink>>& part_sinks)
{
    EXPECT(!base_path.empty(), "Base path must not be empty.");
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");

    const auto paths =
      construct_data_paths(base_path, dimensions, parts_along_dimension);

    return make_s3_sinks(bucket_name, paths, connection_pool, part_sinks);
}
