#include "s3.connection.hh"
#include "unit-test-utils.hh"

#include <cstdlib>
#include <optional>
#include <string_view>

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
        auto conn = std::make_unique<zarr::S3Connection>(settings);

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