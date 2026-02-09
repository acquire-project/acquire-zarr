#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

namespace {
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

    settings->multiscale = false;
}

bool
single_simple_array_at_root_null_key()
{
    ZarrArraySettings array{
        .output_key = nullptr,
    };
    configure_stream_dimensions(&array);

    ZarrStreamSettings settings = {
        .store_path = TEST ".zarr",
        .arrays = &array,
        .array_count = 1,
    };

    ZarrStream* stream = ZarrStream_create(&settings);
    const bool retval = stream != nullptr; // configured correctly
    ZarrStream_destroy(stream);

    ZarrArraySettings_destroy_dimension_array(&array);

    return retval;
}

bool
single_simple_array_at_root_empty_key()
{
    ZarrArraySettings array{
        .output_key = "",
    };
    configure_stream_dimensions(&array);

    ZarrStreamSettings settings = {
        .store_path = TEST ".zarr",
        .arrays = &array,
        .array_count = 1,
    };

    ZarrStream* stream = ZarrStream_create(&settings);
    const bool retval = stream != nullptr; // configured correctly
    ZarrStream_destroy(stream);

    ZarrArraySettings_destroy_dimension_array(&array);

    return retval;
}

// try to configure multiple arrays but with a simple (non-multiscale) array at
// the root (should fail)
bool
multiple_arrays_with_simple_array_at_root()
{
    ZarrStreamSettings settings = {
        TEST ".zarr"
    };

    ZarrStreamSettings_create_arrays(&settings, 2);

    settings.arrays->output_key = nullptr;
    configure_stream_dimensions(settings.arrays);

    settings.arrays[1].output_key = "foo";
    configure_stream_dimensions(settings.arrays + 1);

    ZarrStream* stream = ZarrStream_create(&settings);
    const bool retval = stream == nullptr; // impossible to configure this
    ZarrStream_destroy(stream);

    ZarrStreamSettings_destroy_arrays(&settings); // destroys dimensions

    return retval;
}
} // namespace

int
main()
{
    int retval = 0;

    try {
        if (!single_simple_array_at_root_null_key()) {
            LOG_ERROR(
                "Failed to configure single simple array at root with null key");
            retval = 1;
        }

        if (!single_simple_array_at_root_empty_key()) {
            LOG_ERROR(
                "Failed to configure single simple array at root with empty key");
            retval = 1;
        }

        if (!multiple_arrays_with_simple_array_at_root()) {
            LOG_ERROR(
                "Erroneously successful configuration of multiple arrays with simple array at root");
            retval = 1;
        }
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    return retval;
}