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

    auto config = std::make_shared<zarr::ArrayConfig>();
    config->dtype = ZarrDataType_uint8;
    config->dimensions = dims;
    config->level_of_detail = 0;

    zarr::Downsampler downsampler(config);

    // Check writer configurations
    const auto& writer_configs = downsampler.writer_configurations();
    EXPECT_EQ(size_t, writer_configs.size(), 3);
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

    auto config = std::make_shared<zarr::ArrayConfig>();
    config->dtype = ZarrDataType_uint16;
    config->dimensions = dims;
    config->level_of_detail = 0;

    zarr::Downsampler downsampler(config);

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

        auto config = std::make_shared<zarr::ArrayConfig>();
        config->dtype = type;
        config->dimensions = dims;
        config->level_of_detail = 0;

        // Just test that constructor doesn't throw
        try {
            zarr::Downsampler downsampler(config);

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
        { "z", ZarrDimensionType_Space, 64, 8, 1 },   // Spatial
        { "y", ZarrDimensionType_Space, 512, 64, 1 }, // Spatial
        { "x", ZarrDimensionType_Space, 512, 64, 1 }  // Spatial
    };

    // we need to keep `dimensions` around for assertions
    auto dimensions_to_move = dimensions;

    auto dims = std::make_shared<ArrayDimensions>(std::move(dimensions_to_move),
                                                  ZarrDataType_uint16);

    auto config = std::make_shared<zarr::ArrayConfig>();
    config->dtype = ZarrDataType_uint16;
    config->dimensions = dims;
    config->level_of_detail = 0;

    zarr::Downsampler downsampler(config);
    const auto& configs = downsampler.writer_configurations();

    // We should have some levels based on dimensions
    EXPECT(configs.size() > 0, "No writer configurations were created");

    // Check that spatial dimensions are downsampled
    for (const auto& [level, lvl_config] : configs) {
        if (level == 0)
            continue; // Skip original config

        // Check that non-spatial dimensions are unchanged
        EXPECT_EQ(uint32_t, lvl_config->dimensions->at(0).array_size_px, 100);
        EXPECT_EQ(uint32_t, lvl_config->dimensions->at(1).array_size_px, 3);

        // Check that spatial dimensions are downsampled
        for (auto i = 0; i < 5; ++i) {
            if (i < 2) {
                EXPECT_EQ(uint32_t,
                          lvl_config->dimensions->at(i).array_size_px,
                          dimensions[i].array_size_px);
                continue;
            }

            uint32_t expected = (dimensions[i].array_size_px +
                                 (dimensions[i].array_size_px % 2)) /
                                2;
            for (int j = 1; j < level; ++j) {
                expected = (expected + (expected % 2)) / 2;
            }
            EXPECT_EQ(
              uint32_t, lvl_config->dimensions->at(i).array_size_px, expected);
        }
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

    auto config = std::make_shared<zarr::ArrayConfig>();
    config->dtype = ZarrDataType_uint8;
    config->dimensions = dims;
    config->level_of_detail = 0;

    zarr::Downsampler downsampler(config);

    // Create a test image (11x11)
    ByteVector image(11 * 11, std::byte{ 100 });

    downsampler.add_frame(image);

    ByteVector downsampled;
    bool has_frame = downsampler.get_downsampled_frame(1, downsampled);
    EXPECT(has_frame, "Downsampled frame not found for odd dimensions");

    // Should be padded to 12x12 then downsampled to 6x6
    EXPECT_EQ(size_t, downsampled.size(), 6 * 6);
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
        test_edge_cases();
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    return retval;
}