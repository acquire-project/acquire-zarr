#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <fstream>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / ("test_ragged_downsampling.zarr")).string();

// Original array dimensions
const unsigned int array_width = 64;
const unsigned int array_height = 48;
const unsigned int array_planes = 1;  // Only 1 plane to test non-3D downsampling
const unsigned int array_channels = 3;
const unsigned int array_timepoints = 5;

// Chunk dimensions
const unsigned int chunk_width = 16;
const unsigned int chunk_height = 16;
const unsigned int chunk_planes = 1;
const unsigned int chunk_channels = 3;
const unsigned int chunk_timepoints = 5;

// Shard dimensions
const unsigned int shard_width = 2;
const unsigned int shard_height = 2;
const unsigned int shard_planes = 1;
const unsigned int shard_channels = 1;
const unsigned int shard_timepoints = 1;

// Derived constants
const size_t nbytes_px = sizeof(uint16_t);
const uint32_t frames_to_acquire = array_planes * array_channels * array_timepoints;
const size_t bytes_of_frame = array_width * array_height * nbytes_px;
} // namespace

ZarrStream*
setup()
{
    ZarrArraySettings array = {
        .data_type = ZarrDataType_uint16,
        .multiscale = true,
        .downsampling_method = ZarrDownsamplingMethod_Mean,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .version = ZarrVersion_3,
        .max_threads = 0, // use all available threads
        .arrays = &array,
        .array_count = 1,
    };

    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 2,
        .shuffle = 2,
    };
    settings.arrays->compression_settings = &compression_settings;

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 5));

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
               1.36);

    dim = settings.arrays->dimensions + 3;
    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               chunk_height,
               shard_height,
               "micrometer",
               0.85);

    dim = settings.arrays->dimensions + 4;
    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               chunk_width,
               shard_width,
               "micrometer",
               0.85);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);

    return stream;
}

void verify_multiscale_metadata()
{
    // Read the group metadata
    fs::path group_metadata_path = fs::path(test_path) / "zarr.json";
    EXPECT(fs::is_regular_file(group_metadata_path),
           "Expected file '", group_metadata_path, "' to exist");

    std::ifstream f = std::ifstream(group_metadata_path);
    nlohmann::json group_metadata = nlohmann::json::parse(f);

    // Verify OME-NGFF multiscale metadata
    const auto ome = group_metadata["attributes"]["ome"];
    const auto multiscales = ome["multiscales"][0];
    const auto datasets = multiscales["datasets"];

    // We should have 3 resolution levels (base + 2 downsampled)
    EXPECT_EQ(size_t, datasets.size(), 3);

    for (auto level = 0; level < 3; ++level) {
        const auto& dataset = datasets[level];
        const auto path = dataset["path"].get<std::string>();
        EXPECT(path == std::to_string(level),
               "Expected path to be '", std::to_string(level), "', but got '", path, "'");

        const auto coordinate_transformations = dataset["coordinateTransformations"];
        const auto type = coordinate_transformations[0]["type"].get<std::string>();
        EXPECT(type == "scale", "Expected type to be 'scale', but got '", type, "'");

        const auto scale = coordinate_transformations[0]["scale"];
        EXPECT_EQ(size_t, scale.size(), 5);

        if (level == 0) {
            EXPECT_EQ(double, scale[0].get<double>(), 1.0);
            EXPECT_EQ(double, scale[1].get<double>(), 1.0);
            EXPECT_EQ(double, scale[2].get<double>(), 1.36);
            EXPECT_EQ(double, scale[3].get<double>(), 0.85);
            EXPECT_EQ(double, scale[4].get<double>(), 0.85);
        } else {
            fs::path array_metadata_path = fs::path(test_path) / std::to_string(level) / "zarr.json";
            std::ifstream af = std::ifstream(array_metadata_path);
            nlohmann::json array_metadata = nlohmann::json::parse(af);

            const auto& shape = array_metadata["shape"];

            // Calculate and verify the expected scale factors
            // t and c dimensions should still be 1.0
            EXPECT_EQ(double, scale[0].get<double>(), 1.0); // t dimension
            EXPECT_EQ(double, scale[1].get<double>(), 1.0); // c dimension

            // z dimension should be 1.36 since we have only 1 plane
            EXPECT_EQ(double, scale[2].get<double>(), 1.36);

            // y and x dimensions should match the ratio of original size to downsampled size
            double expected_y_scale =
              0.85 * (array_height / shape[3].get<int>());
            double expected_x_scale =
              0.85 * (array_width / shape[4].get<int>());

            EXPECT(std::abs(scale[3].get<double>() - expected_y_scale) < 0.01,
                   "For level ", level, ", expected y scale to be around ", expected_y_scale,
                   ", but got ", scale[3].get<double>());

            EXPECT(std::abs(scale[4].get<double>() - expected_x_scale) < 0.01,
                   "For level ", level, ", expected x scale to be around ", expected_x_scale,
                   ", but got ", scale[4].get<double>());
        }
    }
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    auto* stream = setup();
    std::vector<uint16_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ", i, ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame);
        }

        ZarrStream_destroy(stream);

        // Verify the multiscale metadata reflects our expectations
        verify_multiscale_metadata();

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