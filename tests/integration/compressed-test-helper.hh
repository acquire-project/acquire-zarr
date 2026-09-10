#pragma once

#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <functional>
#include <vector>

namespace fs = std::filesystem;

namespace compressed_test {

const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();

constexpr unsigned int array_width = 64, array_height = 48,
                       array_planes = 6, array_channels = 8,
                       array_timepoints = 10;

constexpr unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2,
                       chunk_channels = 4, chunk_timepoints = 5;

constexpr unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                       shard_channels = 2, shard_timepoints = 2;
constexpr unsigned int chunks_per_shard =
  shard_width * shard_height * shard_planes * shard_channels * shard_timepoints;

constexpr unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width;
constexpr unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height;
constexpr unsigned int chunks_in_z =
  (array_planes + chunk_planes - 1) / chunk_planes;
constexpr unsigned int chunks_in_c =
  (array_channels + chunk_channels - 1) / chunk_channels;
constexpr unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

constexpr unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width;
constexpr unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height;
constexpr unsigned int shards_in_z =
  (chunks_in_z + shard_planes - 1) / shard_planes;
constexpr unsigned int shards_in_c =
  (chunks_in_c + shard_channels - 1) / shard_channels;
constexpr unsigned int shards_in_t =
  (chunks_in_t + shard_timepoints - 1) / shard_timepoints;

constexpr size_t nbytes_px = sizeof(uint16_t);
constexpr uint32_t frames_to_acquire =
  array_planes * array_channels * array_timepoints;
constexpr size_t bytes_of_frame = array_width * array_height * nbytes_px;

inline ZarrStream*
setup_stream(ZarrCompressionSettings& compression_settings)
{
    ZarrArraySettings array = {
        .data_type = ZarrDataType_uint16,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .max_threads = 0,
        .arrays = &array,
        .array_count = 1,
    };

    settings.arrays->compression_settings = &compression_settings;

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 5));

    ZarrDimensionProperties* dim = settings.arrays->dimensions;
    *dim = DIM("t",
               ZarrDimensionType_Time,
               array_timepoints,
               chunk_timepoints,
               shard_timepoints,
               nullptr,
               1.0);

    dim = settings.arrays->dimensions + 1;
    *dim = DIM("c",
               ZarrDimensionType_Channel,
               array_channels,
               chunk_channels,
               shard_channels,
               nullptr,
               1.0);

    dim = settings.arrays->dimensions + 2;
    *dim = DIM("z",
               ZarrDimensionType_Space,
               array_planes,
               chunk_planes,
               shard_planes,
               "millimeter",
               1.4);

    dim = settings.arrays->dimensions + 3;
    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               chunk_height,
               shard_height,
               "micrometer",
               0.9);

    dim = settings.arrays->dimensions + 4;
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

inline void
verify_file_data()
{
    const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                            chunk_channels * chunk_timepoints * nbytes_px;
    const auto index_size = chunks_per_shard *
                            sizeof(uint64_t) * // indices are 64 bits
                            2;                 // 2 indices per chunk
    const auto checksum_size = 4;              // crc32 checksum is 4 bytes
    const auto expected_file_size = shard_width * shard_height * shard_planes *
                                      shard_channels * shard_timepoints *
                                      chunk_size +
                                    index_size + checksum_size;

    fs::path data_root = fs::path(test_path);

    CHECK(fs::is_directory(data_root));
    for (auto t = 0; t < shards_in_t; ++t) {
        const auto t_dir = data_root / "c" / std::to_string(t);
        CHECK(fs::is_directory(t_dir));

        for (auto c = 0; c < shards_in_c; ++c) {
            const auto c_dir = t_dir / std::to_string(c);
            CHECK(fs::is_directory(c_dir));

            for (auto z = 0; z < shards_in_z; ++z) {
                const auto z_dir = c_dir / std::to_string(z);
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < shards_in_y; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < shards_in_x; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        EXPECT(file_size < expected_file_size,
                               "Expected file size < ",
                               expected_file_size,
                               " for file ",
                               x_file.string(),
                               ", got ",
                               file_size);
                    }
                }
            }
        }
    }
}

using MetadataVerifier = std::function<void(const nlohmann::json&)>;

inline int
run_test(ZarrCompressionSettings& compression_settings,
         MetadataVerifier verify_metadata)
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    auto* stream = setup_stream(compression_settings);
    std::vector<uint16_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame);
        }

        ZarrStream_destroy(stream);

        CHECK(fs::is_directory(test_path));

        {
            fs::path metadata_path = fs::path(test_path) / "zarr.json";
            EXPECT(fs::is_regular_file(metadata_path),
                   "Expected file '",
                   metadata_path,
                   "' to exist");
            std::ifstream f(metadata_path);
            nlohmann::json metadata = nlohmann::json::parse(f);
            verify_metadata(metadata);
        }

        verify_file_data();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    return retval;
}

} // namespace compressed_test
