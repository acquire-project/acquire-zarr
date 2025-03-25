#include "sink.hh"
#include "s3.connection.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <filesystem>

namespace fs = std::filesystem;

namespace {
const std::string test_dir = TEST "-data";

bool
get_settings(zarr::S3Settings& settings)
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        LOG_ERROR("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    settings.endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_ERROR("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    settings.bucket_name = env;

    if (!(env = std::getenv("ZARR_S3_ACCESS_KEY_ID"))) {
        LOG_ERROR("ZARR_S3_ACCESS_KEY_ID not set.");
        return false;
    }
    settings.access_key_id = env;

    if (!(env = std::getenv("ZARR_S3_SECRET_ACCESS_KEY"))) {
        LOG_ERROR("ZARR_S3_SECRET_ACCESS_KEY not set.");
        return false;
    }
    settings.secret_access_key = env;

    env = std::getenv("ZARR_S3_REGION");
    if (env) {
        settings.region = env;
    }

    return true;
}
} // namespace

void
make_v2_metadata_file_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool)
{
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(make_metadata_file_sinks(
      ZarrVersion_2, test_dir, thread_pool, metadata_sinks));

    CHECK(metadata_sinks.size() == 2);
    CHECK(metadata_sinks.contains(".zattrs"));
    CHECK(metadata_sinks.contains(".zgroup"));

    for (auto& [key, sink] : metadata_sinks) {
        CHECK(sink);
        sink.reset(nullptr); // close the file

        fs::path file_path(test_dir + "/" + key);
        EXPECT(fs::is_regular_file(file_path),
               "Metadata file ",
               file_path,
               " not found.");
        // cleanup
        fs::remove(file_path);
    }

    fs::remove(test_dir + "/0");
}

void
make_v2_metadata_s3_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  const std::string& bucket_name)
{
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(zarr::make_metadata_s3_sinks(
      ZarrVersion_2, bucket_name, test_dir, connection_pool, metadata_sinks));

    CHECK(metadata_sinks.size() == 2);
    CHECK(metadata_sinks.contains(".zattrs"));
    CHECK(metadata_sinks.contains(".zgroup"));

    auto conn = connection_pool->get_connection();

    char data_[] = { 0, 0 };
    std::span data(reinterpret_cast<std::byte*>(data_), sizeof(data_));
    for (auto& [key, sink] : metadata_sinks) {
        CHECK(sink);
        // we need to write some data to the sink to ensure it is created
        CHECK(sink->write(0, data));

        CHECK(zarr::finalize_sink(std::move(sink))); // close the connection

        std::string path = test_dir + "/" + key;
        CHECK(conn->object_exists(bucket_name, path));
        // cleanup
        CHECK(conn->delete_object(bucket_name, path));
    }

    CHECK(conn->delete_object(bucket_name, "0"));
}

void
make_v3_metadata_file_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool)
{
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(make_metadata_file_sinks(
      ZarrVersion_3, test_dir, thread_pool, metadata_sinks));

    CHECK(metadata_sinks.size() == 1);
    CHECK(metadata_sinks.contains("zarr.json"));

    for (auto& [key, sink] : metadata_sinks) {
        CHECK(sink);
        sink.reset(nullptr); // close the file

        fs::path file_path(test_dir + "/" + key);
        CHECK(fs::is_regular_file(file_path));
        // cleanup
        fs::remove(file_path);
    }

    fs::remove(test_dir + "/meta");
}

void
make_v3_metadata_s3_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  const std::string& bucket_name)
{
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(zarr::make_metadata_s3_sinks(
      ZarrVersion_3, bucket_name, test_dir, connection_pool, metadata_sinks));

    CHECK(metadata_sinks.size() == 1);
    CHECK(metadata_sinks.contains("zarr.json"));

    auto conn = connection_pool->get_connection();

    char data_[] = { 0, 0 };
    std::span data(reinterpret_cast<std::byte*>(data_), sizeof(data_));
    for (auto& [key, sink] : metadata_sinks) {
        CHECK(sink);
        // we need to write some data to the sink to ensure it is created
        CHECK(sink->write(0, data));
        CHECK(zarr::finalize_sink(std::move(sink))); // close the connection

        std::string path = test_dir + "/" + key;
        CHECK(conn->object_exists(bucket_name, path));
        // cleanup
        CHECK(conn->delete_object(bucket_name, path));
    }

    CHECK(conn->delete_object(bucket_name, "meta"));
}

int
main()
{
    Logger::set_log_level(LogLevel_Debug);

    auto thread_pool = std::make_shared<zarr::ThreadPool>(
      std::thread::hardware_concurrency(),
      [](const std::string& err) { LOG_ERROR("Failed: ", err.c_str()); });

    try {
        make_v2_metadata_file_sinks(thread_pool);
        make_v3_metadata_file_sinks(thread_pool);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    zarr::S3Settings settings;
    if (!get_settings(settings)) {
        LOG_WARNING("Failed to get credentials. Skipping S3 portion of test.");
        return 0;
    }

    auto connection_pool =
      std::make_shared<zarr::S3ConnectionPool>(4, settings);

    try {
        make_v2_metadata_s3_sinks(
          thread_pool, connection_pool, settings.bucket_name);
        make_v3_metadata_s3_sinks(
          thread_pool, connection_pool, settings.bucket_name);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    return 0;
}