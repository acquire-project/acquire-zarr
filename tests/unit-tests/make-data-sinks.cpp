#include "sink.hh"
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

    env = std::getenv("ZARR_S3_REGION");
    if (env) {
        settings.region = env;
    }

    return true;
}
} // namespace

void
make_chunk_file_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool,
                      const ArrayDimensions& dimensions)
{
    // create the sinks, then let them go out of scope to close the handles
    {
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(
          zarr::make_data_file_sinks(test_dir,
                                     dimensions,
                                     zarr::chunks_along_dimension,
                                     thread_pool,
                                     std::make_shared<zarr::FileHandlePool>(),
                                     sinks));

        std::vector<uint8_t> data(2, 0);
        for (auto& sink : sinks) {
            CHECK(sink);
            // we need to write some data to the sink to ensure it is created
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }
    }

    const auto chunks_in_y =
      zarr::chunks_along_dimension(dimensions.height_dim());
    const auto chunks_in_x =
      zarr::chunks_along_dimension(dimensions.width_dim());

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
make_chunk_s3_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool,
                    std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
                    const std::string& bucket_name,
                    const ArrayDimensions& dimensions)
{
    // create the sinks, then let them go out of scope to close the handles
    {
        char data_[] = { 0, 0 };
        std::span data(reinterpret_cast<uint8_t*>(data_), sizeof(data_));
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(make_data_s3_sinks(bucket_name,
                                 test_dir,
                                 dimensions,
                                 zarr::chunks_along_dimension,
                                 connection_pool,
                                 sinks));

        for (auto& sink : sinks) {
            CHECK(sink);
            // we need to write some data to the sink to ensure it is created
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }
    }

    const auto chunks_in_y =
      zarr::chunks_along_dimension(dimensions.height_dim());
    const auto chunks_in_x =
      zarr::chunks_along_dimension(dimensions.width_dim());

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
make_shard_file_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool,
                      const ArrayDimensions& dimensions)
{
    // create the sinks, then let them go out of scope to close the handles
    {
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(make_data_file_sinks(test_dir,
                                   dimensions,
                                   zarr::shards_along_dimension,
                                   thread_pool,
                                   std::make_shared<zarr::FileHandlePool>(),
                                   sinks));

        std::vector<uint8_t> data(2, 0);
        for (auto& sink : sinks) {
            CHECK(sink);
            // we need to write some data to the sink to ensure it is created
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }
    }

    const auto shards_in_y =
      zarr::shards_along_dimension(dimensions.height_dim());
    const auto shards_in_x =
      zarr::shards_along_dimension(dimensions.width_dim());

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
make_shard_s3_sinks(std::shared_ptr<zarr::ThreadPool> thread_pool,
                    std::shared_ptr<zarr::S3ConnectionPool> connection_pool,
                    const std::string& bucket_name,
                    const ArrayDimensions& dimensions)
{
    // create the sinks, then let them go out of scope to close the handles
    {
        char data_[] = { 0, 0 };
        std::span data(reinterpret_cast<uint8_t*>(data_), sizeof(data_));
        std::vector<std::unique_ptr<zarr::Sink>> sinks;
        CHECK(make_data_s3_sinks(bucket_name,
                                 test_dir,
                                 dimensions,
                                 zarr::shards_along_dimension,
                                 connection_pool,
                                 sinks));

        for (auto& sink : sinks) {
            CHECK(sink);
            // we need to write some data to the sink to ensure it is created
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }
    }

    const auto shards_in_y =
      zarr::shards_along_dimension(dimensions.height_dim());
    const auto shards_in_x =
      zarr::shards_along_dimension(dimensions.width_dim());

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
        make_chunk_file_sinks(thread_pool, dimensions);
        make_shard_file_sinks(thread_pool, dimensions);
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
        make_chunk_s3_sinks(
          thread_pool, connection_pool, settings.bucket_name, dimensions);
        make_shard_s3_sinks(
          thread_pool, connection_pool, settings.bucket_name, dimensions);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
        return 1;
    }

    return 0;
}