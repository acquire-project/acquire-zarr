#include "s3.connection.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <optional>

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

        CHECK(conn->bucket_exists(settings.bucket_name));
        CHECK(conn->delete_object(settings.bucket_name, object_name));
        CHECK(!conn->object_exists(settings.bucket_name, object_name));

        std::string upload_id =
          conn->create_multipart_object(settings.bucket_name, object_name);
        CHECK(!upload_id.empty());

        std::vector<zarr::S3Part> parts;

        // parts need to be at least 5MiB, except the last part
        std::vector<uint8_t> data(5 << 20, 0);
        for (auto i = 0; i < 4; ++i) {
            std::string etag = conn->upload_multipart_object_part(
              settings.bucket_name,
              object_name,
              upload_id,
              std::span<uint8_t>(data.data(), data.size()),
              i + 1);
            CHECK(!etag.empty());

            zarr::S3Part part;
            part.number = i + 1;
            part.etag = etag;
            part.size = data.size();

            parts.push_back(part);
        }

        // last part is 1MiB
        {
            const unsigned int part_number = parts.size() + 1;
            const size_t part_size = 1 << 20; // 1MiB
            std::string etag = conn->upload_multipart_object_part(
              settings.bucket_name,
              object_name,
              upload_id,
              std::span<uint8_t>(data.data(), data.size()),
              part_number);
            CHECK(!etag.empty());

            zarr::S3Part part;
            part.number = part_number;
            part.etag = etag;
            part.size = part_size;

            parts.push_back(part);
        }

        CHECK(conn->complete_multipart_object(
          settings.bucket_name, object_name, upload_id, parts));

        CHECK(conn->object_exists(settings.bucket_name, object_name));

        // cleanup
        CHECK(conn->delete_object(settings.bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed: ", e.what());
    }

    return retval;
}