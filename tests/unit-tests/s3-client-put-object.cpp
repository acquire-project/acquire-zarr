#include "s3-test-helper.hh"
#include "unit.test.macros.hh"

#include <vector>

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
        CHECK(client->delete_object(settings->bucket_name, object_name));
        CHECK(!client->object_exists(settings->bucket_name, object_name));

        const std::vector<uint8_t> data(1024, 0);

        CHECK(client->put_object(settings->bucket_name, object_name, data));

        CHECK(client->object_exists(settings->bucket_name, object_name));

        // cleanup
        CHECK(client->delete_object(settings->bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}