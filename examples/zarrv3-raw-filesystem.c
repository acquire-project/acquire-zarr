/// @file zarr-v3-raw-filesystem.c
/// @brief Basic Zarr V3 streaming to filesystem
#include "acquire.zarr.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>

int
main()
{
    // Configure stream settings
    ZarrStreamSettings settings = {
        .store_path = "output_v3.zarr",
        .s3_settings = NULL,
        .compression_settings = NULL,
        .data_type = ZarrDataType_uint16,
        .version = ZarrVersion_3,
    };

    // Set up dimensions (t, y, x)
    ZarrStreamSettings_create_dimension_array(&settings, 3);

    settings.dimensions[0] = (ZarrDimensionProperties){
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 5,
        .shard_size_chunks = 2,
    };

    settings.dimensions[1] = (ZarrDimensionProperties){
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };

    settings.dimensions[2] = (ZarrDimensionProperties){
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
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
    uint16_t* frame = (uint16_t*)malloc(width * height * sizeof(uint16_t));

    // Write frames
    size_t bytes_written;
    for (int t = 0; t < 50; t++) {
        // Fill frame with a moving diagonal pattern
        for (size_t y = 0; y < height; y++) {
            for (size_t x = 0; x < width; x++) {
                // Create a diagonal pattern that moves with time
                // and varies intensity based on position
                int diagonal = (x + y + t * 8) % 32;

                // Create intensity variation
                uint16_t intensity;
                if (diagonal < 16) {
                    intensity = (uint16_t)((diagonal * 4096)); // Ramp up
                } else {
                    intensity = (uint16_t)((31 - diagonal) * 4096); // Ramp down
                }

                // Add some circular features
                int centerX = width / 2;
                int centerY = height / 2;
                int dx = x - centerX;
                int dy = y - centerY;
                int radius = (int)sqrt(dx*dx + dy*dy);

                // Modulate the pattern with concentric circles
                if (radius % 16 < 8) {
                    intensity = (uint16_t)(intensity * 0.7);
                }

                frame[y * width + x] = intensity;
            }
        }

        ZarrStatusCode status = ZarrStream_append(
          stream, frame, width * height * sizeof(uint16_t), &bytes_written);

        if (status != ZarrStatusCode_Success) {
            fprintf(stderr,
                    "Failed to append frame: %s\n",
                    Zarr_get_status_message(status));
            break;
        }
    }

    // Cleanup
    free(frame);
    ZarrStream_destroy(stream);
    return 0;
}