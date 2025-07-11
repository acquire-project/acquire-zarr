#include "downsampler.hh"
#include "unit.test.macros.hh"

namespace {
template<typename T>
zarr::LockedBuffer
create_test_image(size_t width, size_t height, T value = 100)
{
    ByteVector data(width * height * sizeof(T), 0);
    auto* typed_data = reinterpret_cast<T*>(data.data());

    for (size_t i = 0; i < width * height; ++i) {
        typed_data[i] = value;
    }

    return { std::move(data) };
}

void
check_downsample(zarr::Downsampler& downsampler, uint8_t frame_value)
{
    auto first_timepoint = create_test_image<uint8_t>(64, 48, frame_value);
    size_t n_downsampled = 0; // Count how many downsampled frames we expect

    for (auto i = 0; i < 15; ++i) {
        downsampler.add_frame(first_timepoint);
        if (i % 2 == 1) {
            zarr::LockedBuffer downsampled;
            EXPECT(downsampler.take_frame(1, downsampled),
                   "Downsampled frame not found");
            ++n_downsampled;

            downsampled.with_lock([frame_value](const ByteVector& data) {
                for (auto j = 0; j < data.size(); ++j) {
                    auto value = data[j];
                    EXPECT(value == frame_value,
                           "Downsampled value mismatch at timepoint ",
                           j,
                           ": expected ",
                           frame_value,
                           ", got ",
                           value);
                }
            });
        }
    }

    EXPECT(
      n_downsampled == 7, "Expected 7 downsampled frames, got ", n_downsampled);

    zarr::LockedBuffer downsampled;
    EXPECT(downsampler.take_frame(1, downsampled),
           "Downsampled frame not found after all frames added");

    downsampled.with_lock([frame_value](const ByteVector& data) {
        for (auto j = 0; j < data.size(); ++j) {
            auto value = data[j];
            EXPECT(value == frame_value,
                   "Downsampled value mismatch at timepoint ",
                   j,
                   ": expected ",
                   frame_value,
                   ", got ",
                   value);
        }
    });
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
    auto config = std::make_shared<zarr::ArrayConfig>("",
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

        return 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed");
    }

    return retval;
}