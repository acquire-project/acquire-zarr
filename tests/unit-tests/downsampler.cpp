#include "downsampler.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <memory>
#include <vector>

namespace {

// Helper to create simple test images
template<typename T>
ByteVector
create_test_image(size_t width, size_t height, T value = 100)
{
    ByteVector data(width * height * sizeof(T), std::byte{ 0 });
    auto* typed_data = reinterpret_cast<T*>(data.data());

    for (size_t i = 0; i < width * height; ++i) {
        typed_data[i] = value;
    }

    return data;
}

void
test_basic_downsampling()
{
    // Create a simple 2D configuration
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{ { "t", ZarrDimensionType_Time, 0, 5, 1 },
                                  { "y", ZarrDimensionType_Space, 10, 5, 1 },
                                  { "x", ZarrDimensionType_Space, 10, 5, 1 } },
      ZarrDataType_uint8);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint8, 0);

    zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);

    // Check writer configurations
    const auto& writer_configs = downsampler.writer_configurations();
    EXPECT_EQ(size_t, writer_configs.size(), 2);
    EXPECT(writer_configs.count(1) > 0, "Level 1 configuration missing");

    // Create an image with all pixels set to 100
    auto image = create_test_image<uint8_t>(10, 10, 100);

    // Add the frame and check that downsampled version is created
    downsampler.add_frame(image);

    ByteVector downsampled;
    bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(has_frame, "Downsampled frame not found");

    // Verify size (should be 5x5 for uint8)
    EXPECT_EQ(size_t, downsampled.size(), 5 * 5 * sizeof(uint8_t));

    // Verify the downsampled values (should still be 100 since all input pixels
    // were 100)
    auto* typed_downsampled = reinterpret_cast<uint8_t*>(downsampled.data());
    for (size_t i = 0; i < 5 * 5; ++i) {
        EXPECT_EQ(uint8_t, typed_downsampled[i], 100);
    }

    // Check frame is removed from cache after retrieval
    has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(!has_frame, "Downsampled frame was not removed from cache");
}

void
test_3d_downsampling()
{
    // Create a 3D configuration with z as spatial
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{ { "t", ZarrDimensionType_Time, 0, 5, 1 },
                                  { "c", ZarrDimensionType_Channel, 3, 1, 3 },
                                  { "z", ZarrDimensionType_Space, 20, 5, 1 },
                                  { "y", ZarrDimensionType_Space, 20, 5, 1 },
                                  { "x", ZarrDimensionType_Space, 20, 5, 1 } },
      ZarrDataType_uint16);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint16, 0);

    zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);

    // Create test image
    auto image1 = create_test_image<uint16_t>(20, 20, 100);
    auto image2 = create_test_image<uint16_t>(20, 20, 200);
    auto image3 = create_test_image<uint16_t>(20, 20, 300);
    auto image4 = create_test_image<uint16_t>(20, 20, 400);

    // Add first frame - should be stored in partial_scaled_frames_
    downsampler.add_frame(image1);

    ByteVector downsampled;
    bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(!has_frame, "Downsampled frame should not be ready yet in 3D mode");

    // Add second frame - should complete the pair and produce a downsampled
    // frame
    downsampler.add_frame(image2);

    has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(has_frame, "Downsampled frame not found after second frame");

    // Verify the values (should be average of 100 and 200 = 150)
    auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
    for (size_t i = 0; i < 10 * 10; ++i) {
        EXPECT_EQ(uint16_t, typed_downsampled[i], 150);
    }

    // second level shouldn't be ready yet
    has_frame = downsampler.get_downsampled_frame(2, downsampled);
    EXPECT(!has_frame,
           "Downsampled frame should not be ready yet in 3D mode for level 2");

    downsampler.add_frame(image3);
    has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(!has_frame,
           "Downsampled frame should not be ready yet after third frame");

    // second level still shouldn't be ready yet
    has_frame = downsampler.get_downsampled_frame(2, downsampled);
    EXPECT(!has_frame,
           "Downsampled frame should not be ready yet in 3D mode for level 2");

    downsampler.add_frame(image4);

    // now that we've added 4 frames, the second level should be ready
    has_frame = downsampler.get_downsampled_frame(2, downsampled);
    EXPECT(has_frame, "Downsampled frame not found after fourth frame");

    // Verify the values (should be average of 100, 200, 300, and 400 = 250)
    typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
    for (size_t i = 0; i < 5 * 5; ++i) {
        EXPECT_EQ(uint16_t, typed_downsampled[i], 250);
    }
}

void
test_data_types()
{
    // Create a vector of types to test
    std::vector<ZarrDataType> types = {
        ZarrDataType_uint8,  ZarrDataType_uint16, ZarrDataType_uint32,
        ZarrDataType_uint64, ZarrDataType_int8,   ZarrDataType_int16,
        ZarrDataType_int32,  ZarrDataType_int64,  ZarrDataType_float32,
        ZarrDataType_float64
    };

    for (auto type : types) {
        auto dims = std::make_shared<ArrayDimensions>(
          std::vector<ZarrDimension>{
            { "t", ZarrDimensionType_Time, 0, 5, 1 },
            { "y", ZarrDimensionType_Space, 10, 5, 1 },
            { "x", ZarrDimensionType_Space, 10, 5, 1 } },
          type);

        auto config = std::make_shared<zarr::ArrayConfig>(
          "", "/0", std::nullopt, std::nullopt, dims, type, 0);

        // Just test that constructor doesn't throw
        try {
            zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);

            // Add a frame based on the type
            ByteVector image;
            size_t pixel_size = 0;

            switch (type) {
                case ZarrDataType_uint8:
                    image = create_test_image<uint8_t>(10, 10);
                    pixel_size = sizeof(uint8_t);
                    break;
                case ZarrDataType_uint16:
                    image = create_test_image<uint16_t>(10, 10);
                    pixel_size = sizeof(uint16_t);
                    break;
                // Add cases for other types
                case ZarrDataType_uint32:
                    image = create_test_image<uint32_t>(10, 10);
                    pixel_size = sizeof(uint32_t);
                    break;
                case ZarrDataType_uint64:
                    image = create_test_image<uint64_t>(10, 10);
                    pixel_size = sizeof(uint64_t);
                    break;
                case ZarrDataType_int8:
                    image = create_test_image<int8_t>(10, 10);
                    pixel_size = sizeof(int8_t);
                    break;
                case ZarrDataType_int16:
                    image = create_test_image<int16_t>(10, 10);
                    pixel_size = sizeof(int16_t);
                    break;
                case ZarrDataType_int32:
                    image = create_test_image<int32_t>(10, 10);
                    pixel_size = sizeof(int32_t);
                    break;
                case ZarrDataType_int64:
                    image = create_test_image<int64_t>(10, 10);
                    pixel_size = sizeof(int64_t);
                    break;
                case ZarrDataType_float32:
                    image = create_test_image<float>(10, 10);
                    pixel_size = sizeof(float);
                    break;
                case ZarrDataType_float64:
                    image = create_test_image<double>(10, 10);
                    pixel_size = sizeof(double);
                    break;
                default:
                    throw std::runtime_error("Unsupported data type");
            }

            downsampler.add_frame(image);

            ByteVector downsampled;
            bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
            EXPECT(has_frame,
                   "Downsampled frame not found for type " +
                     std::to_string(static_cast<int>(type)));
            EXPECT_EQ(size_t, downsampled.size(), 5 * 5 * pixel_size);

        } catch (const std::exception& e) {
            EXPECT(false,
                   "Failed to create downsampler for type ",
                   static_cast<int>(type),
                   ": ",
                   e.what());
        }
    }
}

void
test_writer_configurations()
{
    // Test with different dimensions to check resolution hierarchy
    std::vector<ZarrDimension> dimensions = {
        { "t", ZarrDimensionType_Time, 100, 10, 1 },  // Non-spatial
        { "c", ZarrDimensionType_Channel, 3, 3, 1 },  // Non-spatial
        { "z", ZarrDimensionType_Space, 128, 8, 1 },  // Spatial
        { "y", ZarrDimensionType_Space, 512, 64, 1 }, // Spatial
        { "x", ZarrDimensionType_Space, 512, 64, 1 }  // Spatial
    };

    // we need to keep `dimensions` around for assertions
    auto dimensions_to_move = dimensions;

    auto dims = std::make_shared<ArrayDimensions>(std::move(dimensions_to_move),
                                                  ZarrDataType_uint16);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint16, 0);

    zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);
    const auto& configs = downsampler.writer_configurations();

    // We should have some levels based on dimensions
    EXPECT(configs.size() == 5,
           "Expected 5 downsampling levels, got ",
           configs.size());

    // Check that spatial dimensions are downsampled
    for (const auto& [level, lvl_config] : configs) {
        if (level == 0) {
            continue; // Skip original config
        }

        const auto& lvl_dims = lvl_config->dimensions;
        // Check that non-spatial dimensions are unchanged
        EXPECT_EQ(uint32_t, lvl_dims->at(0).array_size_px, 100);
        EXPECT_EQ(uint32_t, lvl_dims->at(1).array_size_px, 3);

        // Check that spatial dimensions are downsampled
        for (auto i = 2; i < 5; ++i) {
            uint32_t expected_array_size =
              std::max(dimensions[i].chunk_size_px,
                       dimensions[i].array_size_px / (1 << level));

            EXPECT_EQ(uint32_t,
                      lvl_config->dimensions->at(i).array_size_px,
                      expected_array_size);
        }
    }
}

void
test_anisotropic_writer_configurations()
{
    // Test with different dimensions to check resolution hierarchy
    std::vector<ZarrDimension> dimensions = {
        { "t", ZarrDimensionType_Time, 100, 10, 1 },    // Non-spatial
        { "c", ZarrDimensionType_Channel, 3, 3, 1 },    // Non-spatial
        { "z", ZarrDimensionType_Space, 1000, 128, 1 }, // Spatial
        { "y", ZarrDimensionType_Space, 2000, 512, 1 }, // Spatial
        { "x", ZarrDimensionType_Space, 2000, 256, 1 }  // Spatial
    };

    // we need to keep `dimensions` around for assertions
    auto dimensions_to_move = dimensions;

    auto dims = std::make_shared<ArrayDimensions>(std::move(dimensions_to_move),
                                                  ZarrDataType_uint16);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint16, 0);

    zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);
    const auto& configs = downsampler.writer_configurations();

    // We should have some levels based on dimensions
    EXPECT(configs.size() == 4,
           "Expected 4 downsampling levels, got ",
           configs.size());

    // Check that spatial dimensions & only spatial dimensions are downsampled
    for (const auto& [level, lvl_config] : configs) {
        if (level == 0) {
            continue; // Skip original config
        }

        const auto& lvl_dims = lvl_config->dimensions;
        // Check that non-spatial dimensions are unchanged
        EXPECT_EQ(uint32_t, lvl_dims->at(0).array_size_px, 100);
        EXPECT_EQ(uint32_t, lvl_dims->at(1).array_size_px, 3);
    }

    // Check that spatial dimensions are downsampled
    {
        const auto level = 1;
        const auto lvl_config = configs.at(level);
        const auto& lvl_dims = lvl_config->dimensions;

        EXPECT_EQ(uint32_t, lvl_dims->at(2).array_size_px, 500);
        EXPECT_EQ(uint32_t, lvl_dims->at(2).chunk_size_px, 128);

        EXPECT_EQ(uint32_t, lvl_dims->at(3).array_size_px, 1000);
        EXPECT_EQ(uint32_t, lvl_dims->at(3).chunk_size_px, 512);

        EXPECT_EQ(uint32_t, lvl_dims->at(4).array_size_px, 1000);
        EXPECT_EQ(uint32_t, lvl_dims->at(4).chunk_size_px, 256);
    }

    {
        const auto level = 2;
        const auto lvl_config = configs.at(level);
        const auto& lvl_dims = lvl_config->dimensions;

        EXPECT_EQ(uint32_t, lvl_dims->at(2).array_size_px, 250);
        EXPECT_EQ(uint32_t, lvl_dims->at(2).chunk_size_px, 128);

        EXPECT_EQ(uint32_t, lvl_dims->at(3).array_size_px, 500);
        EXPECT_EQ(uint32_t, lvl_dims->at(3).chunk_size_px, 500);

        EXPECT_EQ(uint32_t, lvl_dims->at(4).array_size_px, 500);
        EXPECT_EQ(uint32_t, lvl_dims->at(4).chunk_size_px, 256);
    }

    {
        const auto level = 3;
        const auto lvl_config = configs.at(level);
        const auto& lvl_dims = lvl_config->dimensions;

        EXPECT_EQ(uint32_t, lvl_dims->at(2).array_size_px, 125);
        EXPECT_EQ(uint32_t, lvl_dims->at(2).chunk_size_px, 125);

        EXPECT_EQ(uint32_t, lvl_dims->at(3).array_size_px, 500);
        EXPECT_EQ(uint32_t, lvl_dims->at(3).chunk_size_px, 500);

        EXPECT_EQ(uint32_t, lvl_dims->at(4).array_size_px, 500);
        EXPECT_EQ(uint32_t, lvl_dims->at(4).chunk_size_px, 256);
    }
}

void
test_edge_cases()
{
    // Test with odd dimensions
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{
        { "t", ZarrDimensionType_Time, 0, 5, 1 },
        { "y", ZarrDimensionType_Space, 11, 5, 1 }, // Odd dimension
        { "x", ZarrDimensionType_Space, 11, 5, 1 }  // Odd dimension
      },
      ZarrDataType_uint8);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint8, 0);

    zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);

    // Create a test image (11x11)
    ByteVector image(11 * 11, std::byte{ 100 });

    downsampler.add_frame(image);

    ByteVector downsampled;
    bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(has_frame, "Downsampled frame not found for odd dimensions");

    // Should be padded to 12x12 then downsampled to 6x6
    EXPECT_EQ(size_t, downsampled.size(), 6 * 6);
}

void
test_min_max_downsampling()
{
    // Create a simple 2D configuration
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{ { "t", ZarrDimensionType_Time, 0, 5, 1 },
                                  { "y", ZarrDimensionType_Space, 10, 5, 1 },
                                  { "x", ZarrDimensionType_Space, 10, 5, 1 } },
      ZarrDataType_uint8);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint8, 0);

    // Create a test image with a pattern that will show different results for min/max/mean
    ByteVector image(10 * 10 * sizeof(uint8_t), std::byte{ 0 });
    auto* typed_data = reinterpret_cast<uint8_t*>(image.data());

    // Create a pattern where each 2x2 block has values [100, 200, 150, 250]
    for (size_t y = 0; y < 10; y += 2) {
        for (size_t x = 0; x < 10; x += 2) {
            typed_data[y * 10 + x] = 100;             // top-left
            typed_data[y * 10 + (x + 1)] = 200;       // top-right
            typed_data[(y + 1) * 10 + x] = 150;       // bottom-left
            typed_data[(y + 1) * 10 + (x + 1)] = 250; // bottom-right
        }
    }

    // Test with mean downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);
        downsampler.add_frame(image);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Mean downsampled frame not found");

        auto* typed_downsampled = reinterpret_cast<uint8_t*>(downsampled.data());
        // For mean, we expect (100 + 200 + 150 + 250) / 4 = 175
        for (size_t i = 0; i < 5 * 5; ++i) {
            EXPECT_EQ(uint8_t, typed_downsampled[i], 175);
        }
    }

    // Test with min downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Min);
        downsampler.add_frame(image);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Min downsampled frame not found");

        auto* typed_downsampled = reinterpret_cast<uint8_t*>(downsampled.data());
        // For min, we expect min(100, 200, 150, 250) = 100
        for (size_t i = 0; i < 5 * 5; ++i) {
            EXPECT_EQ(uint8_t, typed_downsampled[i], 100);
        }
    }

    // Test with max downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Max);
        downsampler.add_frame(image);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Max downsampled frame not found");

        auto* typed_downsampled = reinterpret_cast<uint8_t*>(downsampled.data());
        // For max, we expect max(100, 200, 150, 250) = 250
        for (size_t i = 0; i < 5 * 5; ++i) {
            EXPECT_EQ(uint8_t, typed_downsampled[i], 250);
        }
    }
}

void
test_3d_min_max_downsampling()
{
    // Create a 3D configuration with z as spatial
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{ { "t", ZarrDimensionType_Time, 0, 5, 1 },
                                  { "c", ZarrDimensionType_Channel, 3, 1, 3 },
                                  { "z", ZarrDimensionType_Space, 20, 5, 1 },
                                  { "y", ZarrDimensionType_Space, 20, 5, 1 },
                                  { "x", ZarrDimensionType_Space, 20, 5, 1 } },
      ZarrDataType_uint16);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint16, 0);

    // Test with min downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Min);

        // Create test images with different values
        auto image1 = create_test_image<uint16_t>(20, 20, 100);
        auto image2 = create_test_image<uint16_t>(20, 20, 200);

        downsampler.add_frame(image1);
        downsampler.add_frame(image2);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Min downsampled frame not found after second frame");

        // Verify the values (should be min of 100 and 200 = 100)
        auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
        for (size_t i = 0; i < 10 * 10; ++i) {
            EXPECT_EQ(uint16_t, typed_downsampled[i], 100);
        }
    }

    // Test with max downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Max);

        // Create test images with different values
        auto image1 = create_test_image<uint16_t>(20, 20, 100);
        auto image2 = create_test_image<uint16_t>(20, 20, 200);

        downsampler.add_frame(image1);
        downsampler.add_frame(image2);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Max downsampled frame not found after second frame");

        // Verify the values (should be max of 100 and 200 = 200)
        auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
        for (size_t i = 0; i < 10 * 10; ++i) {
            EXPECT_EQ(uint16_t, typed_downsampled[i], 200);
        }
    }

    // Test multi-level downsampling with max
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Max);

        auto image1 = create_test_image<uint16_t>(20, 20, 100);
        auto image2 = create_test_image<uint16_t>(20, 20, 200);
        auto image3 = create_test_image<uint16_t>(20, 20, 300);
        auto image4 = create_test_image<uint16_t>(20, 20, 400);

        downsampler.add_frame(image1);
        downsampler.add_frame(image2);
        downsampler.add_frame(image3);
        downsampler.add_frame(image4);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(2, downsampled);
        EXPECT(has_frame, "Level 2 max downsampled frame not found");

        // Verify the values (should be max of all values = 400)
        auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
        for (size_t i = 0; i < 5 * 5; ++i) {
            EXPECT_EQ(uint16_t, typed_downsampled[i], 400);
        }
    }
}

void
test_pattern_downsampling()
{
    // Create a 2D configuration
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{ { "t", ZarrDimensionType_Time, 0, 5, 1 },
                                  { "y", ZarrDimensionType_Space, 8, 4, 1 },
                                  { "x", ZarrDimensionType_Space, 8, 4, 1 } },
      ZarrDataType_uint16);

    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, ZarrDataType_uint16, 0);

    // Create a test image with a gradient pattern
    ByteVector image(8 * 8 * sizeof(uint16_t), std::byte{ 0 });
    auto* typed_data = reinterpret_cast<uint16_t*>(image.data());

    // Values increase from left to right and top to bottom
    for (size_t y = 0; y < 8; ++y) {
        for (size_t x = 0; x < 8; ++x) {
            typed_data[y * 8 + x] = static_cast<uint16_t>(100 + x * 20 + y * 50);
        }
    }

    // Get expected results for various methods
    std::vector<uint16_t> expected_mean(4 * 4);
    std::vector<uint16_t> expected_min(4 * 4);
    std::vector<uint16_t> expected_max(4 * 4);

    for (size_t y = 0; y < 4; ++y) {
        for (size_t x = 0; x < 4; ++x) {
            uint16_t v1 = typed_data[(y*2) * 8 + (x*2)];         // top-left
            uint16_t v2 = typed_data[(y*2) * 8 + (x*2 + 1)];     // top-right
            uint16_t v3 = typed_data[(y*2 + 1) * 8 + (x*2)];     // bottom-left
            uint16_t v4 = typed_data[(y*2 + 1) * 8 + (x*2 + 1)]; // bottom-right

            expected_mean[y * 4 + x] = static_cast<uint16_t>((v1 + v2 + v3 + v4) / 4);
            expected_min[y * 4 + x] = std::min(std::min(v1, v2), std::min(v3, v4));
            expected_max[y * 4 + x] = std::max(std::max(v1, v2), std::max(v3, v4));
        }
    }

    // Test with mean downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);
        downsampler.add_frame(image);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Mean downsampled frame not found");

        auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
        for (size_t i = 0; i < 4 * 4; ++i) {
            EXPECT_EQ(uint16_t, typed_downsampled[i], expected_mean[i]);
        }
    }

    // Test with min downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Min);
        downsampler.add_frame(image);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Min downsampled frame not found");

        auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
        for (size_t i = 0; i < 4 * 4; ++i) {
            EXPECT_EQ(uint16_t, typed_downsampled[i], expected_min[i]);
        }
    }

    // Test with max downsampling
    {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Max);
        downsampler.add_frame(image);

        ByteVector downsampled;
        bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
        EXPECT(has_frame, "Max downsampled frame not found");

        auto* typed_downsampled = reinterpret_cast<uint16_t*>(downsampled.data());
        for (size_t i = 0; i < 4 * 4; ++i) {
            EXPECT_EQ(uint16_t, typed_downsampled[i], expected_max[i]);
        }
    }
}
} // namespace zarr::test

int
main()
{
    int retval = 1;

    try {
        test_basic_downsampling();
        test_3d_downsampling();
        test_data_types();
        test_writer_configurations();
        test_anisotropic_writer_configurations();
        test_edge_cases();
        test_min_max_downsampling();
        test_3d_min_max_downsampling();
        test_pattern_downsampling();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    return retval;
}