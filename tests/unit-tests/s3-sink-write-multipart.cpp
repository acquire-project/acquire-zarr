#include "s3.sink.hh"
#include "unit-test-utils.hh"

#include <miniocpp/client.h>

#include <cstdlib>
#include <memory>

int
main()
{
    zarr::S3Settings settings;
    if (!testing::get_s3_settings(settings)) {
        LOG_INFO("Failed to get credentials. Skipping test.");
        return 0;
    }

    int retval = 1;
    const std::string object_name = "test-object";

    try {
        auto pool = std::make_shared<zarr::S3ConnectionPool>(1, settings);

        auto conn = pool->get_connection();
        CHECK(conn->bucket_exists(settings.bucket_name));
        CHECK(conn->delete_object(settings.bucket_name, object_name));
        CHECK(!conn->object_exists(settings.bucket_name, object_name));

        pool->return_connection(std::move(conn));

        std::vector<uint8_t> data((5 << 20) + 1, 0);
        {
            auto sink = std::make_unique<zarr::S3Sink>(
              settings.bucket_name, object_name, pool);
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        conn = pool->get_connection();
        CHECK(conn->object_exists(settings.bucket_name, object_name));
        pool->return_connection(std::move(conn));

        // Verify the object size.
        {
            minio::s3::BaseUrl url(settings.endpoint);
            url.https = settings.endpoint.starts_with("https://");

            minio::creds::EnvAwsProvider provider;
            minio::s3::Client client(url, &provider);
            minio::s3::StatObjectArgs args;
            args.bucket = settings.bucket_name;
            args.object = object_name;

            minio::s3::StatObjectResponse resp = client.StatObject(args);
            EXPECT_EQ(int, data.size(), resp.size);
        }

        // cleanup
        conn = pool->get_connection();
        CHECK(conn->delete_object(settings.bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}