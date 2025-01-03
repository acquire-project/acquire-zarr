/// @file zarrv2-raw-filesystem.c
/// @brief Basic Zarr V2 streaming to filesystem
#include "acquire.zarr.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    // Configure stream settings
    ZarrStreamSettings settings = {
        .store_path = "output_v2.zarr",
        .s3_settings = NULL,
        .compression_settings = NULL,
        .data_type = ZarrDataType_int32,
        .version = ZarrVersion_2
    };

    // Set up dimensions (t, y, x)
    ZarrStreamSettings_create_dimension_array(&settings, 3);

    // Time dimension - unlimited size (0)
    settings.dimensions[0] = (ZarrDimensionProperties){
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 32,
        .shard_size_chunks = 1
    };

    // Y dimension - 48 pixels
    settings.dimensions[1] = (ZarrDimensionProperties){
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1
    };

    // X dimension - 64 pixels
    settings.dimensions[2] = (ZarrDimensionProperties){
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1
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
    const size_t width = 64;
    const size_t height = 48;
    int32_t* frame = (int32_t*)malloc(width * height * sizeof(int32_t));

    // Write some frames
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