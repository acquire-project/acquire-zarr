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

    using Result = zarr::S3Client::PutIfAbsentResult;

    int retval = 1;
    const std::string object_name = "test-object-if-absent";

    try {
        auto client = std::make_unique<zarr::S3Client>(*settings);
        const auto& bucket = settings->bucket_name;

        CHECK(client->bucket_exists(bucket));
        CHECK(client->delete_object(bucket, object_name));
        CHECK(!client->object_exists(bucket, object_name));

        const std::vector<uint8_t> first(16, 1);
        const std::vector<uint8_t> second(32, 2);

        CHECK(client->put_object_if_absent(bucket, object_name, first) ==
              Result::Stored);

        // the second put must not replace the first
        CHECK(client->put_object_if_absent(bucket, object_name, second) ==
              Result::Exists);
        const auto contents = client->get_object(bucket, object_name);
        CHECK(contents.has_value());
        CHECK(*contents == first);

        // cleanup
        CHECK(client->delete_object(bucket, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}
