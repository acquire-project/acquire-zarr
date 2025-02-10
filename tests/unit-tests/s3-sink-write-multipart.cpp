#include "s3.sink.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <memory>

namespace {
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

int
main()
{
    std::string s3_endpoint, bucket_name, s3_access_key_id,
      s3_secret_access_key;
    std::optional<std::string> s3_region;
    if (!get_credentials(s3_endpoint,
                         bucket_name,
                         s3_access_key_id,
                         s3_secret_access_key,
                         s3_region)) {
        LOG_WARNING("Failed to get credentials. Skipping test.");
        return 0;
    }

    int retval = 1;
    const std::string object_name = "test-object";

    try {
        std::shared_ptr<zarr::S3ConnectionPool> pool;
        if (s3_region) {
            pool =
              std::make_shared<zarr::S3ConnectionPool>(1,
                                                       s3_endpoint,
                                                       s3_access_key_id,
                                                       s3_secret_access_key,
                                                       *s3_region);
        } else {
            pool = std::make_shared<zarr::S3ConnectionPool>(
              1, s3_endpoint, s3_access_key_id, s3_secret_access_key);
        }

        auto conn = pool->get_connection();
        if (!conn->is_connection_valid()) {
            LOG_ERROR("Failed to connect to S3.");
            return 1;
        }
        CHECK(conn->bucket_exists(bucket_name));
        CHECK(conn->delete_object(bucket_name, object_name));
        CHECK(!conn->object_exists(bucket_name, object_name));

        pool->return_connection(std::move(conn));

        std::vector<std::byte> data((5 << 20) + 1, std::byte{ 0 });
        {
            auto sink =
              std::make_unique<zarr::S3Sink>(bucket_name, object_name, pool);
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        conn = pool->get_connection();
        CHECK(conn->object_exists(bucket_name, object_name));
        pool->return_connection(std::move(conn));

        // Verify the object size.
        {
            minio::s3::BaseUrl url(s3_endpoint);
            url.https = s3_endpoint.starts_with("https://");

            minio::creds::StaticProvider provider(s3_access_key_id,
                                                  s3_secret_access_key);

            minio::s3::Client client(url, &provider);
            minio::s3::StatObjectArgs args;
            args.bucket = bucket_name;
            args.object = object_name;

            minio::s3::StatObjectResponse resp = client.StatObject(args);
            EXPECT_EQ(int, data.size(), resp.size);
        }

        // cleanup
        conn = pool->get_connection();
        CHECK(conn->delete_object(bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}