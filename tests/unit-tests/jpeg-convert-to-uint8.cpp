#include "jpeg.hh"
#include "unit.test.macros.hh"

#include <cmath>
#include <cstring>
#include <limits>
#include <span>
#include <vector>

namespace {
template<typename T>
std::vector<uint8_t>
convert(const std::vector<T>& values, ZarrDataType dtype, double lo, double hi)
{
    const zarr::JpegParams params{
        .quality = 90,
        .window_min = lo,
        .window_max = hi,
        .source_dtype = dtype,
    };
    std::span<const uint8_t> bytes(
      reinterpret_cast<const uint8_t*>(values.data()),
      values.size() * sizeof(T));
    return zarr::convert_to_uint8(bytes, params);
}

template<typename T>
bool
throws(const std::vector<T>& values, ZarrDataType dtype)
{
    try {
        convert(values, dtype, 0.0, 1.0);
    } catch (const std::exception&) {
        return true;
    }
    return false;
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        // window ends map to 0 and 255; values outside are clamped
        const auto u16 = convert<uint16_t>(
          { 100, 4100, 2100, 0, 65535 }, ZarrDataType_uint16, 100, 4100);
        EXPECT_EQ(int, u16[0], 0);
        EXPECT_EQ(int, u16[1], 255);
        EXPECT_EQ(int, u16[2], 128); // 127.5 rounds to even
        EXPECT_EQ(int, u16[3], 0);
        EXPECT_EQ(int, u16[4], 255);

        // uint8 passes through unchanged, whatever the window
        const auto u8 =
          convert<uint8_t>({ 0, 7, 255 }, ZarrDataType_uint8, 0, 0);
        CHECK(u8 == std::vector<uint8_t>({ 0, 7, 255 }));

        // signed data and a window below zero
        const auto i16 =
          convert<int16_t>({ -1000, 0, 1000 }, ZarrDataType_int16, -1000, 1000);
        EXPECT_EQ(int, i16[0], 0);
        EXPECT_EQ(int, i16[1], 128); // 127.5 rounds to even
        EXPECT_EQ(int, i16[2], 255);

        // finite floats convert; NaN and infinity fail
        const auto f32 =
          convert<float>({ 0.0f, 0.5f, 1.0f }, ZarrDataType_float32, 0, 1);
        EXPECT_EQ(int, f32[1], 128);

        constexpr auto nan32 = std::numeric_limits<float>::quiet_NaN();
        constexpr auto inf64 = std::numeric_limits<double>::infinity();
        CHECK(throws<float>({ 0.0f, nan32 }, ZarrDataType_float32));
        CHECK(throws<double>({ -inf64 }, ZarrDataType_float64));
        CHECK(throws<double>({ inf64 }, ZarrDataType_float64));

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
