#include "s3-test-helper.hh"
#include "unit.test.macros.hh"

int
main()
{
    const auto settings = test::s3_settings_from_env();
    if (!settings) {
        LOG_WARNING("S3 not configured. Skipping test.");
        return 0;
    }

    int retval = 1;

    try {
        auto client = std::make_unique<zarr::S3Client>(*settings);

        if (client->bucket_exists("")) {
            LOG_ERROR("False positive response for empty bucket name.");
            return 1;
        }

        CHECK(client->bucket_exists(settings->bucket_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}