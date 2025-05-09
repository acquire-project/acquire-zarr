#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrStreamSettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrStreamSettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 200,
        .chunk_size_px = 20,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 300,
        .chunk_size_px = 30,
        .shard_size_chunks = 1,
    };
}

ZarrStream*
create_stream_with_metadata(ZarrVersion version)
{
    ZarrStreamSettings settings;
    memset(&settings, 0, sizeof(settings));
    settings.version = version;
    settings.max_threads = 0;
    settings.store_path = TEST ".zarr";

    std::string custom_metadata = R"({"foo":"bar"})";

    configure_stream_dimensions(&settings);

    auto* stream = ZarrStream_create(&settings);
    CHECK(stream);

    // Write custom metadata to the stream
    CHECK(ZarrStatusCode_Success == ZarrStream_write_custom_metadata(
                                      stream, custom_metadata.c_str(), true));

    return stream;
}

ZarrStream*
create_stream_no_metadata(ZarrVersion version)
{
    ZarrStreamSettings settings;
    memset(&settings, 0, sizeof(settings));
    settings.version = version;
    settings.max_threads = 0;
    settings.store_path = TEST ".zarr";

    configure_stream_dimensions(&settings);

    return ZarrStream_create(&settings);
}

void
check_files(bool metadata)
{
    const fs::path base_path(TEST ".zarr");

    if (metadata) {
        CHECK(fs::is_regular_file(base_path / "acquire.json"));
    } else {
        CHECK(!fs::is_regular_file(base_path / "acquire.json"));
    }
}

bool
destroy_directory()
{
    std::error_code ec;
    if (fs::is_directory(TEST ".zarr") && !fs::remove_all(TEST ".zarr", ec)) {
        LOG_ERROR("Failed to remove store path: ", ec.message().c_str());
        return false;
    }

    return true;
}

int
main()
{
    int retval = 1;
    if (!destroy_directory()) {
        return retval;
    }

    try {
        {
            auto* stream = create_stream_no_metadata(ZarrVersion_2);
            CHECK(stream);
            check_files(false);
            ZarrStream_destroy(stream);

            CHECK(destroy_directory());
        }

        {
            auto* stream = create_stream_with_metadata(ZarrVersion_2);
            CHECK(stream);
            check_files(true);
            ZarrStream_destroy(stream);

            CHECK(destroy_directory());
        }

        {
            auto* stream = create_stream_no_metadata(ZarrVersion_3);
            CHECK(stream);
            check_files(false);
            ZarrStream_destroy(stream);

            CHECK(destroy_directory());
        }

        {
            auto* stream = create_stream_with_metadata(ZarrVersion_3);
            CHECK(stream);
            check_files(true);
            ZarrStream_destroy(stream);

            CHECK(destroy_directory());
        }

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    // cleanup
    if (!destroy_directory()) {
        retval = 1;
    }

    return retval;
}