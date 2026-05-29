#include "downsampler.hh"
#include "unit.test.macros.hh"

namespace {
template<typename T>
std::vector<uint8_t>
create_test_image(size_t width, size_t height, T value = 100)
{
    ByteVector data(width * height * sizeof(T), 0);
    auto* typed_data = reinterpret_cast<T*>(data.data());

    for (size_t i = 0; i < width * height; ++i) {
        typed_data[i] = value;
    }

    return std::move(data);
}

// see acquire-project/acquire-zarr#226
void
test_odd_z_multi_tc_no_bleed()
{
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{
        { "t", ZarrDimensionType_Time, 0, 1, 1 },
        { "c", ZarrDimensionType_Channel, 2, 1, 2 },
        { "z", ZarrDimensionType_Space, 3, 1, 1 },
        { "y", ZarrDimensionType_Space, 8, 4, 1 },
        { "x", ZarrDimensionType_Space, 8, 4, 1 },
      },
      ZarrDataType_uint16);
    auto config =
      std::make_shared<zarr::ArrayConfig>("",
                                          "/0",
                                          std::nullopt,
                                          std::nullopt,
                                          dims,
                                          ZarrDataType_uint16,
                                          ZarrDownsamplingMethod_Mean,
                                          0);

    zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);

    const std::vector<uint16_t> channel_values = { 100, 200 };
    constexpr uint32_t T = 2;
    constexpr uint32_t Z = 3;

    std::vector<uint16_t> observed_values;
    for (uint32_t t = 0; t < T; ++t) {
        for (uint16_t value : channel_values) {
            for (uint32_t z = 0; z < Z; ++z) {
                auto frame = create_test_image<uint16_t>(8, 8, value);
                downsampler.add_frame(frame);

                std::vector<uint8_t> downsampled;
                if (downsampler.take_frame(1, downsampled)) {
                    const auto* typed =
                      reinterpret_cast<const uint16_t*>(downsampled.data());
                    const size_t n_pixels =
                      downsampled.size() / sizeof(uint16_t);
                    // Frame is uniform → every pixel matches the first.
                    for (size_t i = 0; i < n_pixels; ++i) {
                        EXPECT_EQ(uint16_t, typed[i], typed[0]);
                    }
                    observed_values.push_back(typed[0]);
                }
            }
        }
    }

    EXPECT_EQ(size_t, observed_values.size(), 8);

    const std::vector<uint16_t> expected = {
        100, 100, // T=0, C=0
        200, 200, // T=0, C=1
        100, 100, // T=1, C=0
        200, 200, // T=1, C=1
    };
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(uint16_t, observed_values[i], expected[i]);
    }

    std::vector<uint8_t> leftover;
    EXPECT(!downsampler.take_frame(1, leftover),
           "Unexpected leftover level-1 frame after all inputs consumed");
}

void
check_downsample(zarr::Downsampler& downsampler, uint8_t frame_value)
{
    auto first_timepoint = create_test_image<uint8_t>(64, 48, frame_value);
    size_t n_downsampled = 0; // Count how many downsampled frames we expect

    for (auto i = 0; i < 15; ++i) {
        downsampler.add_frame(first_timepoint);
        if (i % 2 == 1) {
            std::vector<uint8_t> downsampled;
            EXPECT(downsampler.take_frame(1, downsampled),
                   "Downsampled frame not found");
            ++n_downsampled;

            for (auto j = 0; j < downsampled.size(); ++j) {
                auto value = downsampled[j];
                EXPECT(value == frame_value,
                       "Downsampled value mismatch at timepoint ",
                       j,
                       ": expected ",
                       frame_value,
                       ", got ",
                       value);
            }
        }
    }

    EXPECT(
      n_downsampled == 7, "Expected 7 downsampled frames, got ", n_downsampled);

    std::vector<uint8_t> downsampled;
    EXPECT(downsampler.take_frame(1, downsampled),
           "Downsampled frame not found after all frames added");

    for (auto j = 0; j < downsampled.size(); ++j) {
        auto value = downsampled[j];
        EXPECT(value == frame_value,
               "Downsampled value mismatch at timepoint ",
               j,
               ": expected ",
               frame_value,
               ", got ",
               value);
    }
}
} // namespace

int
main()
{
    int retval = 1;

    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{
        { "t", ZarrDimensionType_Time, 0, 1, 1 },
        { "z", ZarrDimensionType_Space, 15, 3, 1 },
        { "y", ZarrDimensionType_Space, 48, 16, 1 },
        { "x", ZarrDimensionType_Space, 64, 16, 1 },
      },
      ZarrDataType_uint8);
    auto config =
      std::make_shared<zarr::ArrayConfig>("",
                                          "/0",
                                          std::nullopt,
                                          std::nullopt,
                                          dims,
                                          ZarrDataType_uint8,
                                          ZarrDownsamplingMethod_Mean,
                                          0);

    try {
        zarr::Downsampler downsampler(config, ZarrDownsamplingMethod_Mean);
        const auto& writer_configs = downsampler.writer_configurations();
        EXPECT(writer_configs.size() > 1,
               "Expected at least 2 writer configurations, got ",
               writer_configs.size());
        EXPECT(writer_configs.at(1)->dimensions->at(1).array_size_px == 8,
               "Expected downsampled z dimension to be 8, got ",
               writer_configs.at(1)->dimensions->at(1).array_size_px);

        check_downsample(downsampler, 63);
        check_downsample(downsampler, 127);
        check_downsample(downsampler, 255);

        test_odd_z_multi_tc_no_bleed();

        return 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed");
    }

    return retval;
}