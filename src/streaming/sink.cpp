#include "sink.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <algorithm>
#include <filesystem>
#include <latch>
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

        auto job =
          [filename, psink, &latch, &all_successful](std::string& err) -> bool {
            bool success = false;

            try {
                *psink = std::make_unique<zarr::FileSink>(filename);
                success = true;
            } catch (const std::exception& exc) {
                err = "Failed to create file '" + filename + "': " + exc.what();
            }

            latch.count_down();
            all_successful.fetch_and(static_cast<char>(success));

            return success;
        };

        // one thread is reserved for processing the frame queue and runs the
        // entire lifetime of the stream
        if (thread_pool->n_threads() == 1 ||
            !thread_pool->push_job(std::move(job))) {
            std::string err;
            if (!job(err)) {
                LOG_ERROR(err);
            }
        }
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
    std::queue<std::string> paths_queue;
    paths_queue.emplace(base_path);

    // create intermediate paths
    for (auto i = 1;                 // skip the last dimension
         i < dimensions.ndims() - 1; // skip the x dimension
         ++i) {
        const auto& dim = dimensions.at(i);
        const auto n_parts = parts_along_dimension(dim);
        CHECK(n_parts);

        auto n_paths = paths_queue.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths_queue.front();
            paths_queue.pop();

            for (auto k = 0; k < n_parts; ++k) {
                const auto kstr = std::to_string(k);
                paths_queue.push(path + (path.empty() ? kstr : "/" + kstr));
            }
        }
    }

    // create final paths
    std::vector<std::string> paths_out;
    paths_out.reserve(paths_queue.size() *
                      parts_along_dimension(dimensions.width_dim()));
    {
        const auto& dim = dimensions.width_dim();
        const auto n_parts = parts_along_dimension(dim);
        CHECK(n_parts);

        auto n_paths = paths_queue.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths_queue.front();
            paths_queue.pop();
            for (auto j = 0; j < n_parts; ++j)
                paths_out.push_back(path + "/" + std::to_string(j));
        }
    }

    return paths_out;
}

std::vector<std::string>
zarr::get_parent_paths(const std::vector<std::string>& file_paths)
{
    std::unordered_set<std::string> unique_paths;
    for (const auto& file_path : file_paths) {
        unique_paths.emplace(fs::path(file_path).parent_path().string());
    }

    return { unique_paths.begin(), unique_paths.end() };
}

bool
zarr::make_dirs(const std::vector<std::string>& dir_paths,
                std::shared_ptr<ThreadPool> thread_pool)
{
    if (dir_paths.empty()) {
        return true;
    }
    EXPECT(thread_pool, "Thread pool not provided.");

    std::atomic<char> all_successful = 1;

    std::unordered_set<std::string> unique_paths(dir_paths.begin(),
                                                 dir_paths.end());

    auto shared_latch = std::make_shared<std::latch>(unique_paths.size());

    for (const auto& path : unique_paths) {
        auto job = [path, shared_latch, &all_successful](std::string& err) {
            bool success = true;
            if (fs::is_directory(path) || path.empty()) {
                shared_latch->count_down();
                return success;
            }

            std::error_code ec;
            if (!fs::create_directories(path, ec) && !fs::is_directory(path)) {
                err = "Failed to create directory '" + path + "': " + ec.message();
                success = false;
            }

            shared_latch->count_down();
            all_successful.fetch_and(static_cast<char>(success));
            return success;
        };

        // one thread is reserved for processing the frame queue and runs the
        // entire lifetime of the stream
        if (thread_pool->n_threads() == 1 ||
            !thread_pool->push_job(std::move(job))) {
            std::string err;
            if (!job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    shared_latch->wait();
    return static_cast<bool>(all_successful);
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
