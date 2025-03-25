/// @file zarrv3-compressed-multiscale-s3.c
/// @brief Multiscale Zarr V3 with compressed data to S3
#include "acquire.zarr.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

int
main()
{
    // Configure compression
    ZarrCompressionSettings compression = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscZstd,
        .level = 1,
        .shuffle = 1,
    };

    // Configure S3
    ZarrS3Settings s3 = {
        .endpoint = "http://172.29.214.117:9000",
        .bucket_name = "acquire-test",
        .access_key_id = "FBvQC8u3w86djMBmTNEd",
        .secret_access_key = "HAQ6zpagN4em2CGoWgsZcX5R3antE29Gbh4SkHAb",
    };

    // Configure stream settings
    ZarrStreamSettings settings = {
        .store_path = "output_v3_compressed_multiscale_s3.zarr",
        .s3_settings = &s3,
        .compression_settings = &compression,
        .multiscale = true,
        .data_type = ZarrDataType_uint16,
        .version = ZarrVersion_3,
        .max_threads = 0, // use all available threads
    };

    // Set up dimensions (t, y, x)
    ZarrStreamSettings_create_dimension_array(&settings, 3);

    settings.dimensions[0] = (ZarrDimensionProperties){
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
    };

    settings.dimensions[1] = (ZarrDimensionProperties){
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 2048,
        .chunk_size_px = 64,
        .shard_size_chunks = 16,
    };

    settings.dimensions[2] = (ZarrDimensionProperties){
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 2048,
        .chunk_size_px = 64,
        .shard_size_chunks = 16,
    };

    // Create stream
    ZarrStream* stream = ZarrStream_create(&settings);
    // Free Dimension array
    ZarrStreamSettings_destroy_dimension_array(&settings);

    if (!stream) {
        fprintf(stderr, "Failed to create stream\n");
        return 1;
    }

    // Create sample data
    const size_t width = 2048;
    const size_t height = 2048;
    uint16_t* frame = (uint16_t*)malloc(width * height * sizeof(uint16_t));
    for (int i = 0; i < width * height; ++i) {
        frame[i] = (uint16_t)(i % 65536); // Fill with sample data
    }

    // Write frames
    size_t bytes_written;
    for (int t = 0; t < 1024; t++) {
        ZarrStatusCode status = ZarrStream_append(
          stream, frame, width * height * sizeof(uint16_t), &bytes_written);

        if (status != ZarrStatusCode_Success) {
            fprintf(stderr, "Failed to append frame: %s\n",
                    Zarr_get_status_message(status));
            break;
        }
    }

    // Cleanup
    free(frame);
    ZarrStream_destroy(stream);
    return 0;
}