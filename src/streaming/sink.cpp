#include "sink.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <algorithm>
#include <filesystem>
#include <latch>
#include <stdexcept>

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

std::vector<std::string>
construct_metadata_paths(ZarrVersion version)
{
    std::vector<std::string> paths;
    switch (version) {
        case ZarrVersion_2:
            paths.emplace_back(".zattrs");
            paths.emplace_back(".zgroup");
            paths.emplace_back("acquire.json");
            break;
        case ZarrVersion_3:
            paths.emplace_back("zarr.json");
            paths.emplace_back("acquire.json");
            break;
        default:
            throw std::runtime_error("Invalid Zarr version " +
                                     std::to_string(static_cast<int>(version)));
    }

    return paths;
}

bool
make_file_sinks(std::vector<std::string>& file_paths,
                std::shared_ptr<zarr::ThreadPool> thread_pool,
                std::vector<std::unique_ptr<zarr::Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    const auto parents = zarr::get_parent_paths(file_paths);
    if (!zarr::make_dirs(parents, thread_pool)) {
        LOG_ERROR("Failed to make parent directories");
        return false;
    }

    std::atomic<char> all_successful = 1;

    const auto n_files = file_paths.size();
    sinks.resize(n_files);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = file_paths[i];

        std::unique_ptr<zarr::Sink>* psink = sinks.data() + i;

        EXPECT(thread_pool->push_job([filename, psink, &latch, &all_successful](
                                       std::string& err) -> bool {
            bool success = false;

            try {
                if (all_successful) {
                    *psink = std::make_unique<zarr::FileSink>(filename);
                }
                success = true;
            } catch (const std::exception& exc) {
                err = "Failed to create file '" + filename + "': " + exc.what();
            }

            latch.count_down();
            all_successful.fetch_and((char)success);

            return success;
        }),
               "Failed to push job to thread pool.");
    }

    latch.wait();

    return (bool)all_successful;
}

bool
make_file_sinks(
  const std::string& base_dir,
  const std::vector<std::string>& file_paths,
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    // create the parent directories if they don't exist
    const std::string prefix = base_dir.empty() ? "" : base_dir + "/";
    {
        std::vector<std::string> paths_with_parents(file_paths.size());
        for (auto i = 0; i < file_paths.size(); ++i) {
            paths_with_parents[i] = prefix + file_paths[i];
        }

        if (!zarr::make_dirs(zarr::get_parent_paths(paths_with_parents),
                             thread_pool)) {
            LOG_ERROR("Failed to make parent directories");
            return false;
        }
    }

    std::atomic<char> all_successful = 1;

    const auto n_files = file_paths.size();
    std::latch latch(n_files);

    sinks.clear();
    for (const auto& filename : file_paths) {
        sinks[filename] = nullptr;
        std::unique_ptr<zarr::Sink>* psink = &sinks[filename];
        const auto file_path = prefix + filename;

        EXPECT(thread_pool->push_job(
                 [filename = file_path, psink, &latch, &all_successful](
                   std::string& err) -> bool {
                     bool success = false;

                     try {
                         if (all_successful) {
                             *psink =
                               std::make_unique<zarr::FileSink>(filename);
                         }
                         success = true;
                     } catch (const std::exception& exc) {
                         err = "Failed to create file '" + filename +
                               "': " + exc.what();
                     }

                     latch.count_down();
                     all_successful.fetch_and((char)success);

                     return success;
                 }),
               "Failed to push job to thread pool.");
    }

    latch.wait();

    return (bool)all_successful;
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
        sinks[i] = std::make_unique<zarr::S3Sink>(
          bucket_name, object_keys[i], connection_pool);
    }

    return true;
}

bool
make_s3_sinks(
  std::string_view bucket_name,
  std::string_view base_path,
  const std::vector<std::string>& object_keys,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>& sinks)
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

    sinks.clear();
    for (const auto& key : object_keys) {
        sinks[key] = std::make_unique<zarr::S3Sink>(
          bucket_name, std::string(base_path) + "/" + key, connection_pool);
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

std::unique_ptr<zarr::Sink>
zarr::make_file_sink(std::string_view file_path)
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
        if (!fs::create_directories(parent_path, ec)) {
            LOG_ERROR(
              "Failed to create directory '", parent_path, "': ", ec.message());
            return nullptr;
        }
    }

    return std::make_unique<FileSink>(file_path);
}

bool
zarr::make_data_file_sinks(std::string_view base_path,
                           const ArrayDimensions& dimensions,
                           const DimensionPartsFun& parts_along_dimension,
                           std::shared_ptr<ThreadPool> thread_pool,
                           std::vector<std::unique_ptr<Sink>>& part_sinks)
{
    if (base_path.starts_with("file://")) {
        base_path = base_path.substr(7);
    }

    EXPECT(!base_path.empty(), "Base path must not be empty.");

    std::vector<std::string> paths;
    try {
        paths =
          construct_data_paths(base_path, dimensions, parts_along_dimension);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to create dataset paths: ", exc.what());
        return false;
    }

    return make_file_sinks(paths, thread_pool, part_sinks);
}

bool
zarr::make_metadata_file_sinks(
  ZarrVersion version,
  std::string_view base_path,
  std::shared_ptr<ThreadPool> thread_pool,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& metadata_sinks)
{
    if (base_path.starts_with("file://")) {
        base_path = base_path.substr(7);
    }
    EXPECT(!base_path.empty(), "Base path must not be empty.");

    const auto file_paths = construct_metadata_paths(version);

    return make_file_sinks(
      base_path.data(), file_paths, thread_pool, metadata_sinks);
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

    return std::make_unique<S3Sink>(bucket_name, object_key, connection_pool);
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

bool
zarr::make_metadata_s3_sinks(
  ZarrVersion version,
  std::string_view bucket_name,
  std::string_view base_path,
  std::shared_ptr<S3ConnectionPool> connection_pool,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& metadata_sinks)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!base_path.empty(), "Base path must not be empty.");
    if (!bucket_exists(bucket_name, connection_pool)) {
        LOG_ERROR("Bucket '", bucket_name, "' does not exist.");
        return false;
    }

    const auto file_paths = construct_metadata_paths(version);
    return make_s3_sinks(
      bucket_name, base_path, file_paths, connection_pool, metadata_sinks);
}
