#include "macros.hh"
#include "sink.creator.hh"
#include "file.sink.hh"
#include "s3.sink.hh"
#include "acquire.zarr.h"

#include <filesystem>
#include <latch>
#include <queue>
#include <unordered_set>

namespace fs = std::filesystem;

zarr::SinkCreator::SinkCreator(
  std::shared_ptr<zarr::ThreadPool> thread_pool_,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool)
  : thread_pool_{ thread_pool_ }
  , connection_pool_{ connection_pool }
{
}

std::vector<std::string>
zarr::SinkCreator::make_metadata_sink_paths_(size_t version,
                                             std::string_view base_path,
                                             bool create_directories)
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

    if (create_directories) {
        std::queue<std::string> dir_paths;
        dir_paths.emplace(base_path);
        EXPECT(make_dirs_(dir_paths),
               "Failed to create metadata directories.");
        dir_paths.pop(); // remove the base path

        std::unordered_set<std::string> parent_paths;
        for (const auto& path : paths) {
            fs::path parent = fs::path(path).parent_path();
            if (!parent.empty()) {
                parent_paths.emplace((fs::path(base_path) / parent).string());
            }
        }

        for (const auto& dir_path : parent_paths) {
            dir_paths.push(dir_path);
        }

        if (!dir_paths.empty()) {
            EXPECT(make_dirs_(dir_paths),
                   "Failed to create metadata directories.");
        }
    }

    return paths;
}

bool
zarr::SinkCreator::make_dirs_(std::queue<std::string>& dir_paths)
{
    if (dir_paths.empty()) {
        return true;
    }

    std::atomic<char> all_successful = 1;

    const auto n_dirs = dir_paths.size();
    std::latch latch(n_dirs);

    for (auto i = 0; i < n_dirs; ++i) {
        const auto dirname = dir_paths.front();
        dir_paths.pop();

        EXPECT(thread_pool_->push_job(
                 [dirname, &latch, &all_successful](std::string& err) -> bool {
                     if (dirname.empty()) {
                         err = "Directory name must not be empty.";
                         latch.count_down();
                         all_successful.fetch_and(0);
                         return false;
                     }

                     if (fs::is_directory(dirname)) {
                         latch.count_down();
                         return true;
                     } else if (fs::exists(dirname)) {
                         err =
                           "'" + dirname + "' exists but is not a directory";
                         latch.count_down();
                         all_successful.fetch_and(0);
                         return false;
                     }

                     if (all_successful) {
                         std::error_code ec;
                         if (!fs::create_directories(dirname, ec)) {
                             err = "Failed to create directory '" + dirname +
                                   "': " + ec.message();
                             latch.count_down();
                             all_successful.fetch_and(0);
                             return false;
                         }
                     }

                     latch.count_down();
                     return true;
                 }),
               "Failed to push job to thread pool.");

        dir_paths.push(dirname);
    }

    latch.wait();

    return (bool)all_successful;
}

bool
zarr::SinkCreator::bucket_exists_(std::string_view bucket_name)
{
    CHECK(!bucket_name.empty());
    EXPECT(connection_pool_, "S3 connection pool not provided.");

    auto conn = connection_pool_->get_connection();
    bool bucket_exists = conn->bucket_exists(bucket_name);

    connection_pool_->return_connection(std::move(conn));

    return bucket_exists;
}


