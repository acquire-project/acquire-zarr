#include "s3.connection.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <optional>
#include <string_view>

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

        if (!conn->is_connection_valid()) {
            LOG_ERROR("Failed to connect to S3.");
            return 1;
        }

        CHECK(conn->bucket_exists(settings.bucket_name));

        if (conn->object_exists("", object_name)) {
            LOG_ERROR("False positive for empty bucket name.");
            return 1;
        }

        if (conn->object_exists(settings.bucket_name, "")) {
            LOG_ERROR("False positive for empty object name.");
            return 1;
        }

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}