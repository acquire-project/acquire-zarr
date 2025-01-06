#include "sink.creator.hh"
#include "s3.connection.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <filesystem>

namespace fs = std::filesystem;

namespace {
const std::string test_dir = TEST "-data";

bool
get_credentials(std::string& endpoint,
                std::string& bucket_name,
                std::string& access_key_id,
                std::string& secret_access_key)
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        LOG_ERROR("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_ERROR("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    bucket_name = env;

    if (!(env = std::getenv("ZARR_S3_ACCESS_KEY_ID"))) {
        LOG_ERROR("ZARR_S3_ACCESS_KEY_ID not set.");
        return false;
    }
    access_key_id = env;

    if (!(env = std::getenv("ZARR_S3_SECRET_ACCESS_KEY"))) {
        LOG_ERROR("ZARR_S3_SECRET_ACCESS_KEY not set.");
        return false;
    }
    secret_access_key = env;

    return true;
}
} // namespace

void
sink_creator_make_v2_metadata_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool)
{
    zarr::SinkCreator sink_creator(thread_pool, nullptr);

    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(sink_creator.make_metadata_sinks(2, test_dir, metadata_sinks));

    CHECK(metadata_sinks.size() == 3);
    CHECK(metadata_sinks.contains(".zattrs"));
    CHECK(metadata_sinks.contains(".zgroup"));
    CHECK(metadata_sinks.contains("acquire.json"));

    for (auto& [key, sink] : metadata_sinks) {
        CHECK(sink);
        sink.reset(nullptr); // close the file

        fs::path file_path(test_dir + "/" + key);
        CHECK(fs::is_regular_file(file_path));
        // cleanup
        fs::remove(file_path);
    }

    fs::remove(test_dir + "/0");
}

void
sink_creator_make_v2_metadata_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  const std::string& bucket_name)
{
    zarr::SinkCreator sink_creator(thread_pool, connection_pool);

    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(
      sink_creator.make_metadata_sinks(2, bucket_name, test_dir, metadata_sinks));

    CHECK(metadata_sinks.size() == 3);
    CHECK(metadata_sinks.contains(".zattrs"));
    CHECK(metadata_sinks.contains(".zgroup"));
    CHECK(metadata_sinks.contains("acquire.json"));

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
sink_creator_make_v3_metadata_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool)
{
    zarr::SinkCreator sink_creator(thread_pool, nullptr);

    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(sink_creator.make_metadata_sinks(3, test_dir, metadata_sinks));

    CHECK(metadata_sinks.size() == 2);
    CHECK(metadata_sinks.contains("zarr.json"));
    CHECK(metadata_sinks.contains("acquire.json"));

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
sink_creator_make_v3_metadata_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  const std::string& bucket_name)
{
    zarr::SinkCreator sink_creator(thread_pool, connection_pool);

    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>> metadata_sinks;
    CHECK(
      sink_creator.make_metadata_sinks(3, bucket_name, test_dir, metadata_sinks));

    CHECK(metadata_sinks.size() == 2);
    CHECK(metadata_sinks.contains("zarr.json"));
    CHECK(metadata_sinks.contains("acquire.json"));

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
        sink_creator_make_v2_metadata_sinks(thread_pool);
        sink_creator_make_v3_metadata_sinks(thread_pool);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    std::string s3_endpoint, bucket_name, s3_access_key_id,
      s3_secret_access_key;
    if (!get_credentials(
          s3_endpoint, bucket_name, s3_access_key_id, s3_secret_access_key)) {
        LOG_WARNING("Failed to get credentials. Skipping S3 portion of test.");
        return 0;
    }

    auto connection_pool = std::make_shared<zarr::S3ConnectionPool>(
      4, s3_endpoint, s3_access_key_id, s3_secret_access_key);

    try {
        sink_creator_make_v2_metadata_sinks(
          thread_pool, connection_pool, bucket_name);
        sink_creator_make_v3_metadata_sinks(
          thread_pool, connection_pool, bucket_name);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    return 0;
}