#include "s3-test-helper.hh"
#include "s3.sink.hh"
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
        auto client = std::make_shared<zarr::S3Client>(*settings);

        CHECK(client->bucket_exists(settings->bucket_name));
        CHECK(client->delete_object(settings->bucket_name, object_name));
        CHECK(!client->object_exists(settings->bucket_name, object_name));

        // one byte over the part size, so the sink streams rather than sending a
        // single request
        const std::vector<uint8_t> data((5 << 20) + 1, 0);
        {
            auto sink = std::make_unique<zarr::S3Sink>(
              settings->bucket_name, object_name, client);
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        CHECK(client->object_exists(settings->bucket_name, object_name));
        EXPECT_EQ(size_t,
                  data.size(),
                  get_object_size(*client, settings->bucket_name, object_name));

        // cleanup
        CHECK(client->delete_object(settings->bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}
