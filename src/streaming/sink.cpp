#include "sink.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "macros.hh"

#include <algorithm>
#include <filesystem>
#include <latch>
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

    std::latch latch(unique_paths.size());
    for (const auto& path : unique_paths) {
        auto job = [&path, &latch, &all_successful](std::string& err) {
            bool success = true;
            if (fs::is_directory(path)) {
                latch.count_down();
                return success;
            }

            std::error_code ec;
            if (!fs::create_directories(path, ec)) {
                err =
                  "Failed to create directory '" + path + "': " + ec.message();
                success = false;
            }

            latch.count_down();
            all_successful.fetch_and(static_cast<char>(success));

            return success;
        };

        if (!thread_pool->push_job(std::move(job))) {
            LOG_ERROR("Failed to push job to thread pool.");
            return false;
        }
    }

    latch.wait();

    return static_cast<bool>(all_successful);
}
