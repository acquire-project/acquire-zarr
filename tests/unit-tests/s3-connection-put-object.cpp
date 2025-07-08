#include "s3.connection.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <optional>

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
        auto conn = std::make_unique<zarr::S3Connection>(settings);

        CHECK(conn->bucket_exists(settings.bucket_name));
        CHECK(conn->delete_object(settings.bucket_name, object_name));
        CHECK(!conn->object_exists(settings.bucket_name, object_name));

        std::vector<uint8_t> data(1024, 0);

        std::string etag =
          conn->put_object(settings.bucket_name,
                           object_name,
                           std::span<uint8_t>(data.data(), data.size()));
        CHECK(!etag.empty());

        CHECK(conn->object_exists(settings.bucket_name, object_name));

        // cleanup
        CHECK(conn->delete_object(settings.bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}