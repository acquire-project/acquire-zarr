/// @file stream-multiarray-to-filesystem.cpp
/// @brief Stream multiple arrays with different data types to filesystem
#include "acquire.zarr.h"

#include <cstdint> // for uint16_t, uint8_t
#include <cstdlib> // for rand()
#include <iostream>
#include <vector>

template<typename T>
void
fill_data(std::vector<uint8_t>& data)
{
    T* data_view = reinterpret_cast<T*>(data.data());
    size_t n_elements = data.size() / sizeof(T);

    for (size_t i = 0; i < n_elements; ++i) {
        data_view[i] = static_cast<T>(rand() % 65536);
    }
}

int
main()
{
    // Configure compression settings for different arrays
    ZarrCompressionSettings lz4_compression = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 1,
        .shuffle = 1,
    };

    ZarrCompressionSettings zstd_compression = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscZstd,
        .level = 3,
        .shuffle = 2,
    };

    // Configure stream settings
    ZarrStreamSettings settings = {
        .store_path = "output_multiarray.zarr",
        .s3_settings = nullptr,
        .version = ZarrVersion_3,
        .max_threads = 0, // use all available threads
        .overwrite = true,
    };

    // Allocate arrays
    ZarrStatusCode status = ZarrStreamSettings_create_arrays(&settings, 3);
    if (status != ZarrStatusCode_Success) {
        std::cerr << "Failed to create arrays: "
                  << Zarr_get_status_message(status) << std::endl;
        return 1;
    }

    // Array 1: 5D uint16 array with LZ4 compression
    settings.arrays[0] = {
        .output_key = "path/to/uint16_array",
        .compression_settings = &lz4_compression,
        .data_type = ZarrDataType_uint16,
    };

    // Array 2: 3D float32 array with Zstd compression
    settings.arrays[1] = {
        .output_key = "a/float32/array",
        .compression_settings = &zstd_compression,
        .data_type = ZarrDataType_float32,
    };

    // Array 3: 3D uint8 array with no compression
    settings.arrays[2] = {
        .output_key = "labels",
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint8,
    };

    // Set up dimensions for Array 1: 5D (t, c, z, y, x)
    ZarrArraySettings_create_dimension_array(&settings.arrays[0], 5);
    settings.arrays[0].dimensions[0] = {
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 5,
        .shard_size_chunks = 2,
    };
    settings.arrays[0].dimensions[1] = {
        .name = "c",
        .type = ZarrDimensionType_Channel,
        .array_size_px = 8,
        .chunk_size_px = 4,
        .shard_size_chunks = 2,
    };
    settings.arrays[0].dimensions[2] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 6,
        .chunk_size_px = 2,
        .shard_size_chunks = 1,
    };
    settings.arrays[0].dimensions[3] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };
    settings.arrays[0].dimensions[4] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
    };

    // Set up dimensions for Array 2: 3D (z, y, x)
    ZarrArraySettings_create_dimension_array(&settings.arrays[1], 3);
    settings.arrays[1].dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 6,
        .chunk_size_px = 2,
        .shard_size_chunks = 1,
    };
    settings.arrays[1].dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };
    settings.arrays[1].dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
    };

    // Set up dimensions for Array 3: 3D (z, y, x)
    ZarrArraySettings_create_dimension_array(&settings.arrays[2], 3);
    settings.arrays[2].dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 6,
        .chunk_size_px = 2,
        .shard_size_chunks = 1,
    };
    settings.arrays[2].dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };
    settings.arrays[2].dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
    };

    // Create stream
    ZarrStream* stream = ZarrStream_create(&settings);

    if (!stream) {
        fprintf(stderr, "Failed to create stream\n");
        // Free dimension arrays before returning
        for (int i = 0; i < 3; i++) {
            ZarrArraySettings_destroy_dimension_array(&settings.arrays[i]);
        }
        ZarrStreamSettings_destroy_arrays(&settings);
        return 1;
    }

    // Create and write sample data for Array 1 (uint16, 5D)
    size_t uint16_size = 10 * 8 * 6 * 48 * 64;
    std::vector<uint8_t> uint16_data(uint16_size * sizeof(uint16_t));
    fill_data<uint16_t>(uint16_data);

    size_t bytes_written;
    status = ZarrStream_append(stream,
                               uint16_data.data(),
                               uint16_size * sizeof(uint16_t),
                               &bytes_written,
                               "path/to/uint16_array");

    if (status != ZarrStatusCode_Success) {
        std::cerr << "Failed to append uint16 data: "
                  << Zarr_get_status_message(status) << std::endl;
    }

    // Create and write sample data for Array 2 (float32, 3D)
    size_t float32_size = 6 * 48 * 64;
    std::vector<uint8_t> float32_data(float32_size * sizeof(float));
    fill_data<float>(float32_data);

    status = ZarrStream_append(stream,
                               float32_data.data(),
                               float32_size * sizeof(float),
                               &bytes_written,
                               "a/float32/array");

    if (status != ZarrStatusCode_Success) {
        std::cerr << "Failed to append float32 data: "
                  << Zarr_get_status_message(status) << std::endl;
    }

    // Create and write sample data for Array 3 (uint8, 3D)
    size_t uint8_size = 6 * 48 * 64;
    std::vector<uint8_t> uint8_data(uint8_size * sizeof(uint8_t));
    fill_data<uint8_t>(uint8_data);

    status = ZarrStream_append(stream,
                               uint8_data.data(),
                               uint8_size * sizeof(uint8_t),
                               &bytes_written,
                               "labels");

    if (status != ZarrStatusCode_Success) {
        std::cerr << "Failed to append uint8 data: "
                  << Zarr_get_status_message(status) << std::endl;
    }

    // Free dimension arrays
    for (int i = 0; i < 3; i++) {
        ZarrArraySettings_destroy_dimension_array(&settings.arrays[i]);
    }
    ZarrStreamSettings_destroy_arrays(&settings);

    // Tear down the stream
    ZarrStream_destroy(stream);

    return 0;
}