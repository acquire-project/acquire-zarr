#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();

const unsigned int array_width = 2048, array_height = 2048,
                   array_timepoints = 1024;

const unsigned int chunk_width = 64, chunk_height = 64, chunk_timepoints = 64;

const unsigned int shard_width = 16, shard_height = 16, shard_timepoints = 1;
const unsigned int chunks_per_shard =
  shard_width * shard_height * shard_timepoints;

const unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
const unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks
const unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

const unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
const unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
const unsigned int shards_in_t =
  (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

const size_t nbytes_px = sizeof(uint16_t);
const uint32_t frames_to_acquire = array_timepoints;
const size_t bytes_of_frame = array_width * array_height * nbytes_px;
} // namespace/s

ZarrStream*
setup()
{
    ZarrArraySettings array = {
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint16,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .version = ZarrVersion_3,
        .max_threads = 0, // use all available threads
        .arrays = &array,
        .array_count = 1,
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 3));

    ZarrDimensionProperties* dim;
    dim = settings.arrays->dimensions;
    *dim = DIM("t",
               ZarrDimensionType_Time,
               array_timepoints,
               chunk_timepoints,
               shard_timepoints,
               nullptr,
               1.0);

    dim = settings.arrays->dimensions + 1;
    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               chunk_height,
               shard_height,
               "micrometer",
               0.9);

    dim = settings.arrays->dimensions + 2;
    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               chunk_width,
               shard_width,
               "micrometer",
               0.9);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);

    return stream;
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Info);

    auto* stream = setup();
    std::vector<uint16_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            auto start_time = std::chrono::high_resolution_clock::now();
            const ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
            auto end_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed = end_time - start_time;
            LOG_INFO("Appending frame ",
                     i,
                     "/",
                     frames_to_acquire,
                     " took ",
                     elapsed.count() * 1000,
                     " ms");
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame);
        }

        ZarrStream_destroy(stream);

        // Clean up
        fs::remove_all(test_path);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    // cleanup
    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    return retval;
}
