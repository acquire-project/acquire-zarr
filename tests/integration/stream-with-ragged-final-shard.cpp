#include "acquire.zarr.h"
#include "test.macros.hh"

#include <filesystem>
#include <stdexcept>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

namespace fs = std::filesystem;

namespace {
size_t
align_to(size_t size, size_t align)
{
    if (align == 0) {
        return size;
    }
    return (size + align - 1) & ~(align - 1);
}

size_t
align_to_system_size(size_t size, const std::string& path)
{
#ifdef _WIN32

    // get page size
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    EXPECT(si.dwPageSize > 0, "Could not get system page size");
    size_t page_size = si.dwPageSize;

    // get sector size
    char volume_path[MAX_PATH];
    EXPECT(GetVolumePathNameA(path.c_str(), volume_path, MAX_PATH),
           "Failed to get volume name for path '",
           path,
           "'");

    DWORD sectors_per_cluster;
    DWORD bytes_per_sector;
    DWORD number_of_free_clusters;
    DWORD total_number_of_clusters;

    EXPECT(GetDiskFreeSpaceA(volume_path,
                             &sectors_per_cluster,
                             &bytes_per_sector,
                             &number_of_free_clusters,
                             &total_number_of_clusters),
           "Failed to get disk free space for volume: " +
             std::string(volume_path));

    EXPECT(bytes_per_sector > 0, "Could not get sector size");

    size_t sector_size = bytes_per_sector;

    return align_to(align_to(size, page_size), sector_size);

#else
    return size; // no additional alignment needed on POSIX
#endif
}
}

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

        const size_t expected_full_shard_size =
          16 * align_to_system_size(expected_chunk_size, first_shard.string()) +
          table_size;
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

        const size_t expected_partial_shard_size =
          align_to_system_size(expected_chunk_size, last_shard.string()) +
          table_size;
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