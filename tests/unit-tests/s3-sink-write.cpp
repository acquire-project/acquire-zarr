#include "s3-test-helper.hh"
#include "s3.sink.hh"
#include "unit.test.macros.hh"

#include <span>

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
    const std::string expected = "Hello, Acquire!";

    try {
        auto client = std::make_shared<zarr::S3Client>(*settings);

        CHECK(client->bucket_exists(settings->bucket_name));
        CHECK(client->delete_object(settings->bucket_name, object_name));
        CHECK(!client->object_exists(settings->bucket_name, object_name));

        {
            auto sink = std::make_unique<zarr::S3Sink>(
              settings->bucket_name, object_name, client);
            std::span data{ reinterpret_cast<const uint8_t*>(expected.data()),
                            expected.size() };
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        CHECK(client->object_exists(settings->bucket_name, object_name));

        const auto contents =
          get_object_contents(*client, settings->bucket_name, object_name);
        if (contents != expected) {
            LOG_ERROR("Expected '", expected, "' but got '", contents, "'");
            return 1;
        }

        // cleanup
        CHECK(client->delete_object(settings->bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}
