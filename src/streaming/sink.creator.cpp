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

std::unique_ptr<zarr::Sink>
zarr::SinkCreator::make_sink(std::string_view file_path)
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
            LOG_ERROR("Failed to create directory '", parent_path, "': ",
                      ec.message());
            return nullptr;
        }
    }

    return std::make_unique<FileSink>(file_path);
}

std::unique_ptr<zarr::Sink>
zarr::SinkCreator::make_sink(std::string_view bucket_name,
                             std::string_view object_key)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_key.empty(), "Object key must not be empty.");
    EXPECT(connection_pool_, "S3 connection pool not provided.");
    if (!bucket_exists_(bucket_name)) {
        LOG_ERROR("Bucket '", bucket_name, "' does not exist.");
        return nullptr;
    }

    return std::make_unique<S3Sink>(bucket_name, object_key, connection_pool_);
}

bool
zarr::SinkCreator::make_data_sinks(
  std::string_view base_path,
  const ArrayDimensions* dimensions,
  const std::function<size_t(const ZarrDimension&)>& parts_along_dimension,
  std::vector<std::unique_ptr<Sink>>& part_sinks)
{
    if (base_path.starts_with("file://")) {
        base_path = base_path.substr(7);
    }

    EXPECT(!base_path.empty(), "Base path must not be empty.");

    std::queue<std::string> paths;
    try {
        paths = make_data_sink_paths_(
          base_path, dimensions, parts_along_dimension, true);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to create dataset paths: ", exc.what());
        return false;
    }

    return make_files_(paths, part_sinks);
}

bool
zarr::SinkCreator::make_data_sinks(
  std::string_view bucket_name,
  std::string_view base_path,
  const ArrayDimensions* dimensions,
  const std::function<size_t(const ZarrDimension&)>& parts_along_dimension,
  std::vector<std::unique_ptr<Sink>>& part_sinks)
{
    EXPECT(!base_path.empty(), "Base path must not be empty.");

    auto paths = make_data_sink_paths_(
      base_path, dimensions, parts_along_dimension, false);

    return make_s3_objects_(bucket_name, paths, part_sinks);
}

bool
zarr::SinkCreator::make_metadata_sinks(
  size_t version,
  std::string_view base_path,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& metadata_sinks)
{
    if (base_path.starts_with("file://")) {
        base_path = base_path.substr(7);
    }
    EXPECT(!base_path.empty(), "Base path must not be empty.");

    std::vector<std::string> file_paths =
      make_metadata_sink_paths_(version, base_path, true);

    return make_files_(base_path.data(), file_paths, metadata_sinks);
}

bool
zarr::SinkCreator::make_metadata_sinks(
  size_t version,
  std::string_view bucket_name,
  std::string_view base_path,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& metadata_sinks)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!base_path.empty(), "Base path must not be empty.");
    if (!bucket_exists_(bucket_name)) {
        LOG_ERROR("Bucket '", bucket_name, "' does not exist.");
        return false;
    }

    auto file_paths = make_metadata_sink_paths_(version, base_path, false);
    return make_s3_objects_(bucket_name, base_path, file_paths, metadata_sinks);
}

std::queue<std::string>
zarr::SinkCreator::make_data_sink_paths_(
  std::string_view base_path,
  const ArrayDimensions* dimensions,
  const std::function<size_t(const ZarrDimension&)>& parts_along_dimension,
  bool create_directories)
{
    std::queue<std::string> paths;
    paths.emplace(base_path);

    if (create_directories) {
        EXPECT(
          make_dirs_(paths), "Failed to create directory '", base_path, "'.");
    }

    // create intermediate paths
    for (auto i = 1;                // skip the last dimension
         i < dimensions->ndims() - 1; // skip the x dimension
         ++i) {
        const auto& dim = dimensions->at(i);
        const auto n_parts = parts_along_dimension(dim);
        CHECK(n_parts);

        auto n_paths = paths.size();
        for (auto j = 0; j < n_paths; ++j) {
            const auto path = paths.front();
            paths.pop();

            for (auto k = 0; k < n_parts; ++k) {
                const auto kstr = std::to_string(k);
                paths.push(path + (path.empty() ? kstr : "/" + kstr));
            }
        }

        if (create_directories) {
            EXPECT(make_dirs_(paths),
                   "Failed to create directories for dimension '",
                   dim.name,
                   "'.");
        }
    }

    // create final paths
    {
        const auto& dim = dimensions->width_dim();
        const auto n_parts = parts_along_dimension(dim);
        CHECK(n_parts);

        auto n_paths = paths.size();
        for (auto i = 0; i < n_paths; ++i) {
            const auto path = paths.front();
            paths.pop();
            for (auto j = 0; j < n_parts; ++j)
                paths.push(path + "/" + std::to_string(j));
        }
    }

    return paths;
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
zarr::SinkCreator::make_files_(std::queue<std::string>& file_paths,
                               std::vector<std::unique_ptr<Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<char> all_successful = 1;

    const auto n_files = file_paths.size();
    sinks.resize(n_files);
    std::fill(sinks.begin(), sinks.end(), nullptr);
    std::latch latch(n_files);

    for (auto i = 0; i < n_files; ++i) {
        const auto filename = file_paths.front();
        file_paths.pop();

        std::unique_ptr<Sink>* psink = sinks.data() + i;

        EXPECT(thread_pool_->push_job(
                 [filename, psink, &latch, &all_successful](
                   std::string& err) -> bool {
                     bool success = false;

                     try {
                         if (all_successful) {
                             *psink = std::make_unique<FileSink>(filename);
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
zarr::SinkCreator::make_files_(
  const std::string& base_dir,
  const std::vector<std::string>& file_paths,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& sinks)
{
    if (file_paths.empty()) {
        return true;
    }

    std::atomic<char> all_successful = 1;

    const auto n_files = file_paths.size();
    std::latch latch(n_files);

    sinks.clear();
    for (const auto& filename : file_paths) {
        sinks[filename] = nullptr;
        std::unique_ptr<Sink>* psink = &sinks[filename];

        const std::string prefix = base_dir.empty() ? "" : base_dir + "/";
        const auto file_path = prefix + filename;

        EXPECT(thread_pool_->push_job(
                 [filename = file_path, psink, &latch, &all_successful](
                   std::string& err) -> bool {
                     bool success = false;

                     try {
                         if (all_successful) {
                             *psink = std::make_unique<FileSink>(filename);
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
zarr::SinkCreator::bucket_exists_(std::string_view bucket_name)
{
    CHECK(!bucket_name.empty());
    EXPECT(connection_pool_, "S3 connection pool not provided.");

    auto conn = connection_pool_->get_connection();
    bool bucket_exists = conn->bucket_exists(bucket_name);

    connection_pool_->return_connection(std::move(conn));

    return bucket_exists;
}

bool
zarr::SinkCreator::make_s3_objects_(std::string_view bucket_name,
                                    std::queue<std::string>& object_keys,
                                    std::vector<std::unique_ptr<Sink>>& sinks)
{
    if (object_keys.empty()) {
        return true;
    }

    if (bucket_name.empty()) {
        LOG_ERROR("Bucket name not provided.");
        return false;
    }
    if (!connection_pool_) {
        LOG_ERROR("S3 connection pool not provided.");
        return false;
    }

    const auto n_objects = object_keys.size();
    sinks.resize(n_objects);
    for (auto i = 0; i < n_objects; ++i) {
        sinks[i] = std::make_unique<S3Sink>(
          bucket_name, object_keys.front(), connection_pool_);
        object_keys.pop();
    }

    return true;
}

bool
zarr::SinkCreator::make_s3_objects_(
  std::string_view bucket_name,
  std::string_view base_path,
  std::vector<std::string>& object_keys,
  std::unordered_map<std::string, std::unique_ptr<Sink>>& sinks)
{
    if (object_keys.empty()) {
        return true;
    }

    if (bucket_name.empty()) {
        LOG_ERROR("Bucket name not provided.");
        return false;
    }

    if (!connection_pool_) {
        LOG_ERROR("S3 connection pool not provided.");
        return false;
    }

    sinks.clear();
    for (const auto& key : object_keys) {
        sinks[key] = std::make_unique<S3Sink>(
          bucket_name, std::string(base_path) + "/" + key, connection_pool_);
    }

    return true;
}
