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
                           const DimensionPartsFun& parts_along_dimension,
                           bool make_directories)
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
                const auto dirname = path + (path.empty() ? kstr : "/" + kstr);
                paths_queue.push(dirname);

                if (make_directories) {
                    fs::create_directories(dirname);
                }
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

std::unique_ptr<zarr::Sink>
zarr::make_file_sink(std::string_view file_path,
                     std::shared_ptr<FileHandlePool> file_handle_pool,
                     bool truncate_to_fit)
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

    return std::make_unique<FileSink>(
      file_path, file_handle_pool, truncate_to_fit);
}

std::unique_ptr<zarr::Sink>
zarr::make_s3_sink(std::string_view bucket_name,
                   std::string_view object_key,
                   std::shared_ptr<S3Client> client)
{
    EXPECT(!object_key.empty(), "Object key must not be empty.");

    // the bucket is checked once when the stream is created, not per object
    return std::make_unique<S3Sink>(bucket_name, object_key, client);
}
