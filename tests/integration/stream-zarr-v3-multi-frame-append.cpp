#include "acquire.zarr.h"
#include "test.macros.hh"
#include <filesystem>
#include <vector>
#include <fstream>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;

static const size_t array_width = 64;
static const size_t array_height = 48;
static const size_t frames_to_acquire = 12;
static const size_t frames_per_append = 3;
static const fs::path test_path = "multi-frame-test.zarr";

static ZarrStream* 
setup() {
    const auto test_path_str = test_path.string();
    const auto test_path_cstr = test_path_str.c_str();

    // Clean up any existing test directory first
    if (fs::exists(test_path)) {
        LOG_DEBUG("Removing existing test directory");
        fs::remove_all(test_path);
    }
    LOG_DEBUG("Creating Zarr store at: ", test_path_str);

    ZarrStreamSettings settings = {  
        .store_path = test_path_cstr,
        .data_type = ZarrDataType_uint16,
        .version = ZarrVersion_3,
    };

    CHECK(ZarrStatusCode_Success == 
          ZarrStreamSettings_create_dimension_array(&settings, 3));

    // Configure dimensions [t, y, x]
    settings.dimensions[0] = {
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0, // Append dimension
        .chunk_size_px = 5,
        .shard_size_chunks = 2,
    };

    settings.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_height,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
    };

    settings.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_width,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
    };

    auto* stream = ZarrStream_create(&settings);
    ZarrStreamSettings_destroy_dimension_array(&settings);
    CHECK(stream != nullptr);
    return stream;
}

static void
verify_data() {
    LOG_DEBUG("Verifying data at ", test_path);
    // Basic structure verification
    CHECK(fs::exists(test_path));
    CHECK(fs::exists(test_path / "zarr.json")); // Check group metadata exists
    
    // Verify the array metadata and final number of frames
    const fs::path array_metadata_path = test_path / "0" / "zarr.json";
    CHECK(fs::exists(array_metadata_path));
    CHECK(fs::file_size(array_metadata_path) > 0);

    // Read and parse the array metadata
    std::ifstream f(array_metadata_path);
    nlohmann::json array_metadata = nlohmann::json::parse(f);
    
    // Verify the shape - first dimension should match our frames
    const auto& shape = array_metadata["shape"];
    EXPECT_EQ(size_t, shape.size(), 3); // [t, y, x]
    EXPECT_EQ(size_t, shape[0].get<size_t>(), frames_to_acquire); // time dimension
    EXPECT_EQ(size_t, shape[1].get<size_t>(), array_height);
    EXPECT_EQ(size_t, shape[2].get<size_t>(), array_width);
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);
    LOG_DEBUG("Starting multi-frame append test");

    auto* stream = setup();
    if (!stream) {
        LOG_ERROR("Failed to create ZarrStream");
        return 1;
    }

    LOG_DEBUG("ZarrStream created successfully");
    const size_t frame_size = array_width * array_height * sizeof(uint16_t);
    const size_t multi_frame_size = frame_size * frames_per_append;
    
    std::vector<uint16_t> multi_frame_data(array_width * array_height * frames_per_append);
    int retval = 1;

    try {
        // Test: Append multiple complete frames
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; i += frames_per_append) {
            LOG_DEBUG("Appending frames ", i, " to ", i + frames_per_append - 1);
            
            // Fill multi-frame buffer with test pattern
            for (size_t f = 0; f < frames_per_append; ++f) {
                const size_t frame_offset = f * array_width * array_height;
                const uint16_t frame_value = static_cast<uint16_t>(i + f);
                std::fill(multi_frame_data.begin() + frame_offset,
                         multi_frame_data.begin() + frame_offset + (array_width * array_height),
                         frame_value);
            }

            ZarrStatusCode status = ZarrStream_append(
                stream,
                multi_frame_data.data(),
                multi_frame_size,
                &bytes_out);

            if (status != ZarrStatusCode_Success) {
                LOG_ERROR("Failed to append frames. Status: ", status);
                throw std::runtime_error("ZarrStream_append failed");
            }

            LOG_DEBUG("Successfully appended ", bytes_out, " bytes");
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frames ",
                   i,
                   "-",
                   i + frames_per_append - 1);
            EXPECT_EQ(size_t, bytes_out, multi_frame_size);
        }

        LOG_DEBUG("All frames appended successfully, destroying stream");
        ZarrStream_destroy(stream);

        verify_data();

        // Clean up
        LOG_DEBUG("Test completed successfully, cleaning up");
        // fs::remove_all(test_path);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    return retval;
} 