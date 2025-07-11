/// @file zarrv2-compressed-s3.c
/// @brief Zarr V2 with ZSTD compression to S3
#include "acquire.zarr.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    // Configure S3
    // Ensure that you have set your S3 credentials in the environment variables
    // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and optionally AWS_SESSION_TOKEN
    ZarrS3Settings s3 = {
        .endpoint = "http://localhost:9000",
        .bucket_name = "mybucket",
    };

    // Configure compression
    ZarrCompressionSettings compression = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscZstd,
        .level = 1,
        .shuffle = 1
    };

    // Configure stream settings
    ZarrStreamSettings settings = {
        .store_path = "output_v2_s3.zarr",
        .s3_settings = &s3,
        .compression_settings = &compression,
        .data_type = ZarrDataType_int32,
        .version = ZarrVersion_2,
        .max_threads = 0, // use all available threads
    };

    // Set up dimensions (t, c, y, x)
    ZarrArraySettings_create_dimension_array(&settings.array, 4);

    settings.array.dimensions[0] = (ZarrDimensionProperties){
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,  // Unlimited
        .chunk_size_px = 32,
        .shard_size_chunks = 1
    };

    settings.array.dimensions[1] = (ZarrDimensionProperties){
        .name = "c",
        .type = ZarrDimensionType_Channel,
        .array_size_px = 3,
        .chunk_size_px = 3,
        .shard_size_chunks = 1
    };

    settings.array.dimensions[2] = (ZarrDimensionProperties){
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1
    };

    settings.array.dimensions[3] = (ZarrDimensionProperties){
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1
    };

    // Create stream
    ZarrStream* stream = ZarrStream_create(&settings);
    // Free Dimension array
    ZarrArraySettings_destroy_dimension_array(&settings.array);

    if (!stream) {
        fprintf(stderr, "Failed to create stream\n");
        return 1;
    }

    // Create sample data
    const size_t width = 64;
    const size_t height = 48;
    int32_t* frame = (int32_t*)malloc(width * height * sizeof(int32_t));

    // Write frames
    size_t bytes_written;
    for (int i = 0; i < 10; i++) {
        // Fill frame with sample data
        for (size_t j = 0; j < width * height; j++) {
            frame[j] = i * 1000 + j;
        }

        ZarrStatusCode status = ZarrStream_append(
          stream,
          frame,
          width * height * sizeof(int32_t),
          &bytes_written
        );

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
