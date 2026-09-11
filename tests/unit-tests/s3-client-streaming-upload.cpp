#include "s3-test-helper.hh"
#include "unit.test.macros.hh"

#include <span>
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

        // enough to force the CRT to split the upload into parts, with a final
        // part smaller than S3's 5 MiB minimum
        const size_t part_size = 5 << 20;
        const size_t n_full_parts = 4;
        const size_t tail_size = 1 << 20;
        const size_t total = n_full_parts * part_size + tail_size;

        {
            auto upload =
              client->create_upload(settings->bucket_name, object_name);
            CHECK(upload);

            const std::vector<uint8_t> chunk(part_size, 0);
            for (auto i = 0; i < n_full_parts; ++i) {
                CHECK(upload->append(chunk));
            }
            CHECK(upload->append(
              std::span(chunk.data(), tail_size)));

            CHECK(upload->finish());
        }

        CHECK(client->object_exists(settings->bucket_name, object_name));
        EXPECT_EQ(size_t,
                  total,
                  get_object_size(*client, settings->bucket_name, object_name));

        // cleanup
        CHECK(client->delete_object(settings->bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}
