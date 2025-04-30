#include "unit.test.macros.hh"
#include "zarr.stream.hh"

void configure_new_group(ZarrStream* stream)
{
    ZarrGroupProperties properties = {
        .store_key = "new_group",
        .data_type = ZarrDataType_uint16,
        .multiscale = false,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
    };

    properties.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 0,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    properties.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    properties.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    CHECK(stream->configure_group(&properties) == ZarrStatusCode_Success);
}

int
main()
{
    int retval = 1;

    ZarrStreamSettings settings{
        .store_path = TEST,
        .s3_settings = nullptr,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
        .multiscale = false,
        .data_type = ZarrDataType_uint8,
        .version = ZarrVersion_3,
        .max_threads = 0,
    };

    settings.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    settings.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    settings.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    ZarrStream* stream;
    try {
        stream = new ZarrStream(&settings);
        configure_new_group(stream);
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Error: ", e.what());
    }

    delete[] settings.dimensions;

    finalize_stream(stream);
    delete stream;

    return retval;
}