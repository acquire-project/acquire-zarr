#include "sink.creator.hh"
#include "s3.connection.hh"
#include "zarr.common.hh"
#include "acquire.zarr.h"
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
                std::string& secret_access_key,
                std::optional<std::string>& region)
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

    env = std::getenv("ZARR_S3_REGION");
    if (env) {
        region = env;
    }

    return true;
}
} // namespace

void
sink_creator_make_chunk_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool,
                              const ArrayDimensions* dimensions)
{
    zarr::SinkCreator sink_creator(thread_pool, nullptr);

    // create the sinks, then let them go out of scope to close the handles
    {
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(sink_creator.make_data_sinks(
          test_dir, dimensions, zarr::chunks_along_dimension, sinks));
    }

    const auto chunks_in_y = zarr::chunks_along_dimension(dimensions->height_dim());
    const auto chunks_in_x = zarr::chunks_along_dimension(dimensions->width_dim());

    const fs::path base_path(test_dir);
    for (auto i = 0; i < chunks_in_y; ++i) {
        const fs::path y_dir = base_path / std::to_string(i);

        for (auto j = 0; j < chunks_in_x; ++j) {
            const fs::path x_file = y_dir / std::to_string(j);
            CHECK(fs::is_regular_file(x_file));

            // cleanup
            fs::remove(x_file);
        }
        CHECK(!fs::is_regular_file(y_dir / std::to_string(chunks_in_x)));
        fs::remove(y_dir);
    }
    CHECK(!fs::is_directory(base_path / std::to_string(chunks_in_y)));
}

void
sink_creator_make_chunk_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  const std::string& bucket_name,
  const ArrayDimensions* dimensions)
{
    zarr::SinkCreator sink_creator(thread_pool, connection_pool);

    // create the sinks, then let them go out of scope to close the handles
    {
        char data_[] = { 0, 0 };
        std::span data(reinterpret_cast<std::byte*>(data_), sizeof(data_));
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(sink_creator.make_data_sinks(bucket_name,
                                           test_dir,
                                           dimensions,
                                           zarr::chunks_along_dimension,
                                           sinks));

        for (auto& sink : sinks) {
            CHECK(sink);
            // we need to write some data to the sink to ensure it is created
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }
    }

    const auto chunks_in_y = zarr::chunks_along_dimension(dimensions->height_dim());
    const auto chunks_in_x = zarr::chunks_along_dimension(dimensions->width_dim());

    auto conn = connection_pool->get_connection();

    const std::string base_path(test_dir);
    for (auto i = 0; i < chunks_in_y; ++i) {
        const std::string y_dir = base_path + "/" + std::to_string(i);

        for (auto j = 0; j < chunks_in_x; ++j) {
            const std::string x_file = y_dir + "/" + std::to_string(j);
            CHECK(conn->object_exists(bucket_name, x_file));

            // cleanup
            CHECK(conn->delete_object(bucket_name, x_file));
        }
        CHECK(!conn->object_exists(bucket_name,
                                   y_dir + "/" + std::to_string(chunks_in_x)));
        CHECK(conn->delete_object(bucket_name, y_dir));
    }
    CHECK(!conn->object_exists(bucket_name,
                               base_path + "/" + std::to_string(chunks_in_y)));
    CHECK(conn->delete_object(bucket_name, base_path));
}

void
sink_creator_make_shard_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool,
                              const ArrayDimensions* dimensions)
{
    zarr::SinkCreator sink_creator(thread_pool, nullptr);

    // create the sinks, then let them go out of scope to close the handles
    {
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(sink_creator.make_data_sinks(
          test_dir, dimensions, zarr::shards_along_dimension, sinks));
    }

    const auto shards_in_y = zarr::shards_along_dimension(dimensions->height_dim());
    const auto shards_in_x = zarr::shards_along_dimension(dimensions->width_dim());

    const fs::path base_path(test_dir);
    for (auto i = 0; i < shards_in_y; ++i) {
        const fs::path y_dir = base_path / std::to_string(i);

        for (auto j = 0; j < shards_in_x; ++j) {
            const fs::path x_file = y_dir / std::to_string(j);
            CHECK(fs::is_regular_file(x_file));

            // cleanup
            fs::remove(x_file);
        }
        CHECK(!fs::is_regular_file(y_dir / std::to_string(shards_in_x)));
        fs::remove(y_dir);
    }
    CHECK(!fs::is_directory(base_path / std::to_string(shards_in_y)));
}

void
sink_creator_make_shard_sinks(
  std::shared_ptr<zarr::ThreadPool> thread_pool,
  std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
  const std::string& bucket_name,
  const ArrayDimensions* dimensions)
{
    zarr::SinkCreator sink_creator(thread_pool, connection_pool);

    // create the sinks, then let them go out of scope to close the handles
    {
        char data_[] = { 0, 0 };
        std::span data(reinterpret_cast<std::byte*>(data_), sizeof(data_));
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(sink_creator.make_data_sinks(bucket_name,
                                           test_dir,
                                           dimensions,
                                           zarr::shards_along_dimension,
                                           sinks));

        for (auto& sink : sinks) {
            CHECK(sink);
            // we need to write some data to the sink to ensure it is created
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }
    }

    const auto shards_in_y = zarr::shards_along_dimension(dimensions->height_dim());
    const auto shards_in_x = zarr::shards_along_dimension(dimensions->width_dim());

    auto conn = connection_pool->get_connection();

    const std::string base_path(test_dir);
    for (auto i = 0; i < shards_in_y; ++i) {
        const std::string y_dir = base_path + "/" + std::to_string(i);

        for (auto j = 0; j < shards_in_x; ++j) {
            const std::string x_file = y_dir + "/" + std::to_string(j);
            CHECK(conn->object_exists(bucket_name, x_file));

            // cleanup
            CHECK(conn->delete_object(bucket_name, x_file));
        }
        CHECK(!conn->object_exists(bucket_name,
                                   y_dir + "/" + std::to_string(shards_in_x)));
        CHECK(conn->delete_object(bucket_name, y_dir));
    }
    CHECK(!conn->object_exists(bucket_name,
                               base_path + "/" + std::to_string(shards_in_y)));
    CHECK(conn->delete_object(bucket_name, base_path));
}

int
main()
{
    Logger::set_log_level(LogLevel_Debug);

    std::vector<ZarrDimension> dims;
    dims.emplace_back("z",
                      ZarrDimensionType_Space,
                      0,
                      3,  // 3 planes per chunk
                      1); // 1 chunk per shard (3 planes per shard)
    dims.emplace_back("y",
                      ZarrDimensionType_Space,
                      4,
                      2,  // 2 rows per chunk, 2 chunks
                      2); // 2 chunks per shard (4 rows per shard, 1 shard)
    dims.emplace_back("x",
                      ZarrDimensionType_Space,
                      12,
                      3,  // 3 columns per chunk, 4 chunks
                      2); // 2 chunks per shard (6 columns per shard, 2 shards)
    ArrayDimensions dimensions(std::move(dims), ZarrDataType_int8);

    auto thread_pool = std::make_shared<zarr::ThreadPool>(
      std::thread::hardware_concurrency(),
      [](const std::string& err) { LOG_ERROR("Failed: ", err.c_str()); });

    try {
        sink_creator_make_chunk_sinks(thread_pool, &dimensions);
        sink_creator_make_shard_sinks(thread_pool, &dimensions);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    std::string s3_endpoint, bucket_name, s3_access_key_id,
      s3_secret_access_key;
    std::optional<std::string> s3_region;
    if (!get_credentials(s3_endpoint,
                         bucket_name,
                         s3_access_key_id,
                         s3_secret_access_key,
                         s3_region)) {
        LOG_WARNING("Failed to get credentials. Skipping S3 portion of test.");
        return 0;
    }

    std::shared_ptr<zarr::S3ConnectionPool> connection_pool;
    if (s3_region) {
        connection_pool =
          std::make_shared<zarr::S3ConnectionPool>(4,
                                                   s3_endpoint,
                                                   s3_access_key_id,
                                                   s3_secret_access_key,
                                                   *s3_region);
    } else {
        connection_pool = std::make_shared<zarr::S3ConnectionPool>(
          4, s3_endpoint, s3_access_key_id, s3_secret_access_key);
    }

    try {
        sink_creator_make_chunk_sinks(
          thread_pool, connection_pool, bucket_name, &dimensions);
        sink_creator_make_shard_sinks(
          thread_pool, connection_pool, bucket_name, &dimensions);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    return 0;
}