#include "s3.connection.hh"
#include "unit-test-utils.hh"

#include <cstdlib>
#include <optional>

int
main()
{
    zarr::S3Settings settings;
    if (!testing::get_s3_settings(settings)) {
        LOG_INFO("Failed to get credentials. Skipping test.");
        return 0;
    }

    int retval = 1;

    try {
        auto conn = std::make_unique<zarr::S3Connection>(settings);

        if (conn->bucket_exists("")) {
            LOG_ERROR("False positive response for empty bucket name.");
            return 1;
        }

        CHECK(conn->bucket_exists(settings.bucket_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}