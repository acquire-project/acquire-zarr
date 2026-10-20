#include "s3-test-helper.hh"
#include "unit.test.macros.hh"
#include <string_view>

int
main()
{
    const auto settings = test::s3_settings_from_env();
    if (!settings) {
        LOG_WARNING("S3 not configured. Skipping test.");
        return 0;
    }

    int retval = 1;
    const std::string object_name = "test-object";

    try {
        auto client = std::make_unique<zarr::S3Client>(*settings);

        CHECK(client->bucket_exists(settings->bucket_name));

        if (client->object_exists("", object_name)) {
            LOG_ERROR("False positive for empty bucket name.");
            return 1;
        }

        if (client->object_exists(settings->bucket_name, "")) {
            LOG_ERROR("False positive for empty object name.");
            return 1;
        }

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}