#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
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

int
main()
{
    int retval = 1;

    ZarrStream* stream = nullptr;
    ZarrStreamSettings settings = {};
    settings.max_threads = std::thread::hardware_concurrency();

    try {
        // try to create a stream with no store path
        stream = ZarrStream_create(&settings);
        CHECK(nullptr == stream);

        // try to create a stream with no dimensions
        settings.store_path = static_cast<const char*>(TEST ".zarr");
        stream = ZarrStream_create(&settings);
        CHECK(nullptr == stream);
        CHECK(!fs::exists(settings.store_path));

        // allocate dimensions
        CHECK(ZarrStatusCode_Success ==
              ZarrStreamSettings_create_arrays(&settings, 1));
        configure_stream_dimensions(settings.arrays);
        stream = ZarrStream_create(&settings);
        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings.store_path));

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    // cleanup
    ZarrStream_destroy(stream);

    if (std::error_code ec; fs::is_directory(settings.store_path) &&
                            !fs::remove_all(settings.store_path, ec)) {
        LOG_ERROR("Failed to remove store path: ", ec.message().c_str());
    }

    ZarrStreamSettings_destroy_arrays(&settings);
    return retval;
}