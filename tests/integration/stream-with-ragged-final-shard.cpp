#include "acquire.zarr.h"
#include "test.macros.hh"

#include <filesystem>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

int
main()
{
    try {
        // Configure stream settings
        ZarrArraySettings array = {
            .compression_settings = nullptr,
            .data_type = ZarrDataType_uint8,
        };
        ZarrStreamSettings settings = {
            .store_path = TEST ".zarr",
            .s3_settings = nullptr,
            .version = ZarrVersion_3,
            .max_threads = 0, // use all available threads
            .overwrite = true,
            .arrays = &array,
            .array_count = 1,
        };

        ZarrArraySettings_create_dimension_array(settings.arrays, 5);

        settings.arrays->dimensions[0] = {
            .name = "t",
            .type = ZarrDimensionType_Time,
            .array_size_px = 0,
            .chunk_size_px = 1,
            .shard_size_chunks = 16,
        };

        settings.arrays->dimensions[1] = {
            .name = "c",
            .type = ZarrDimensionType_Channel,
            .array_size_px = 1,
            .chunk_size_px = 1,
            .shard_size_chunks = 1,
        };

        settings.arrays->dimensions[2] = {
            .name = "z",
            .type = ZarrDimensionType_Space,
            .array_size_px = 125,
            .chunk_size_px = 125,
            .shard_size_chunks = 1,
        };

        settings.arrays->dimensions[3] = {
            .name = "y",
            .type = ZarrDimensionType_Space,
            .array_size_px = 125,
            .chunk_size_px = 125,
            .shard_size_chunks = 1,
        };

        settings.arrays->dimensions[4] = {
            .name = "x",
            .type = ZarrDimensionType_Space,
            .array_size_px = 125,
            .chunk_size_px = 125,
            .shard_size_chunks = 1,
        };

        // Create stream
        ZarrStream* stream = ZarrStream_create(&settings);
        // Free Dimension array
        ZarrArraySettings_destroy_dimension_array(settings.arrays);

        EXPECT(stream, "Failed to create stream");

        // Create sample data
        constexpr size_t width = 125;
        constexpr size_t height = 125;
        constexpr size_t planes = 125;
        std::vector<uint8_t> stack(width * height * planes, 0);

        // Write frames
        size_t bytes_written;
        for (int t = 0; t < 17; t++) {
            ZarrStatusCode status = ZarrStream_append(
              stream, stack.data(), stack.size(), &bytes_written, nullptr);

            EXPECT(
              status == ZarrStatusCode_Success, "Failed to append stack ", t);
        }

        ZarrStream_destroy(stream);

        constexpr size_t expected_chunk_size = width * height * planes;
        const size_t table_size = 2 * 16 * sizeof(uint64_t) + 4;

        const fs::path first_shard(TEST ".zarr/0/c/0/0/0/0/0");
        EXPECT(fs::is_regular_file(first_shard),
               "Expected shard file does not exist: ",
               first_shard.string());

        constexpr auto expected_full_shard_size =
          16 * expected_chunk_size + table_size;
        EXPECT(fs::file_size(first_shard) == expected_full_shard_size,
               "Expected ",
               first_shard.string(),
               " to be ",
               expected_full_shard_size,
               " bytes, got ",
               fs::file_size(first_shard));

        const fs::path last_shard(TEST ".zarr/0/c/1/0/0/0/0");
        EXPECT(fs::is_regular_file(last_shard),
               "Expected shard file does not exist: ",
               last_shard.string());

        constexpr auto expected_partial_shard_size =
          expected_chunk_size + table_size;
        EXPECT(fs::file_size(last_shard) == expected_partial_shard_size,
               "Expected ",
               last_shard.string(),
               " to be ",
               expected_partial_shard_size,
               " bytes, got ",
               fs::file_size(last_shard));
    } catch (const std::exception& err) {
        LOG_ERROR("Failed: ", err.what());
        return 1;
    }

    return 0;
}