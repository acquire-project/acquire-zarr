#include "s3.sink.hh"
#include "unit.test.macros.hh"

#include <miniocpp/client.h>

#include <cstdlib>

namespace {
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

int
main()
{
    zarr::S3Settings settings;
    if (!get_settings(settings)) {
        LOG_WARNING("Failed to get credentials. Skipping test.");
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

        {
            char str[] = "Hello, Acquire!";
            auto sink = std::make_unique<zarr::S3Sink>(
              settings.bucket_name, object_name, pool);
            std::span data{ reinterpret_cast<std::byte*>(str),
                            sizeof(str) - 1 };
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        conn = pool->get_connection();
        CHECK(conn->object_exists(settings.bucket_name, object_name));
        pool->return_connection(std::move(conn));

        // Verify the object contents.
        {
            minio::s3::BaseUrl url(settings.endpoint);
            url.https = settings.endpoint.starts_with("https://");

            minio::creds::EnvAwsProvider provider;
            minio::s3::Client client(url, &provider);
            minio::s3::GetObjectArgs args;
            args.bucket = settings.bucket_name;
            args.object = object_name;

            std::string contents;
            args.datafunc =
              [&contents](minio::http::DataFunctionArgs args) -> bool {
                contents = args.datachunk;
                return true;
            };

            // Call get object.
            minio::s3::GetObjectResponse resp = client.GetObject(args);

            if (contents != "Hello, Acquire!") {
                LOG_ERROR(
                  "Expected 'Hello, Acquire!' but got '", contents, "'");
                return 1;
            }
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