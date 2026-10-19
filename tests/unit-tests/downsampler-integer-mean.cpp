#include "downsampler.hh"
#include "unit.test.macros.hh"

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

/**
 * Mean downsampling of integer data must equal the exact mean truncated toward
 * zero. For 2D data within float64 precision, that is what the recipe in the
 * multiscales metadata gives (downscale_local_mean, then a cast). Values near the ends
 * of the range make the 2x2 (mean4) and z-pair (mean2) sums overflow T, so
 * they catch an implementation that sums in T (acquire-project/acquire-zarr#265).
 */
namespace {
template<typename T>
using Quad = std::array<T, 4>;

template<typename T>
using Pair = std::array<T, 2>;

template<typename T>
ZarrDataType
dtype_of()
{
    if constexpr (std::is_same_v<T, uint8_t>) {
        return ZarrDataType_uint8;
    } else if constexpr (std::is_same_v<T, uint16_t>) {
        return ZarrDataType_uint16;
    } else if constexpr (std::is_same_v<T, uint32_t>) {
        return ZarrDataType_uint32;
    } else if constexpr (std::is_same_v<T, uint64_t>) {
        return ZarrDataType_uint64;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return ZarrDataType_int8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return ZarrDataType_int16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return ZarrDataType_int32;
    } else {
        static_assert(std::is_same_v<T, int64_t>);
        return ZarrDataType_int64;
    }
}

template<typename T>
std::string
str(T value)
{
    return std::to_string(static_cast<std::conditional_t<std::is_signed_v<T>,
                                                         int64_t,
                                                         uint64_t>>(value));
}

template<typename T>
std::shared_ptr<zarr::ArrayConfig>
make_config(bool with_z)
{
    std::vector<ZarrDimension> dims{ { "t", ZarrDimensionType_Time, 0, 1, 1 } };
    if (with_z) {
        dims.push_back({ "z", ZarrDimensionType_Space, 2, 1, 1 });
    }
    dims.push_back({ "y", ZarrDimensionType_Space, 4, 2, 1 });
    dims.push_back({ "x", ZarrDimensionType_Space, 4, 2, 1 });

    return std::make_shared<zarr::ArrayConfig>(
      "",
      "/0",
      std::nullopt,
      std::nullopt,
      std::make_shared<ArrayDimensions>(std::move(dims), dtype_of<T>()),
      dtype_of<T>(),
      ZarrDownsamplingMethod_Mean,
      0,
      true);
}

/** A 4x4 frame whose four 2x2 blocks hold the four quadruples. */
template<typename T>
std::vector<uint8_t>
make_frame(const std::array<Quad<T>, 4>& blocks)
{
    std::vector<uint8_t> frame(16 * sizeof(T));
    auto* px = reinterpret_cast<T*>(frame.data());
    for (size_t b = 0; b < 4; ++b) {
        const size_t row = 2 * (b / 2), col = 2 * (b % 2);
        px[row * 4 + col] = blocks[b][0];
        px[row * 4 + col + 1] = blocks[b][1];
        px[(row + 1) * 4 + col] = blocks[b][2];
        px[(row + 1) * 4 + col + 1] = blocks[b][3];
    }
    return frame;
}

template<typename T>
std::vector<T>
level_1(std::vector<uint8_t>& frame_data)
{
    std::vector<T> out(frame_data.size() / sizeof(T));
    memcpy(out.data(), frame_data.data(), frame_data.size());
    return out;
}

template<typename T>
void
check_mean4(const Quad<T>& in, T expected)
{
    zarr::Downsampler downsampler(make_config<T>(false),
                                  ZarrDownsamplingMethod_Mean);
    auto frame = make_frame<T>({ in, in, in, in });
    downsampler.add_frame(frame);

    std::vector<uint8_t> data;
    CHECK(downsampler.take_frame(1, data));
    const auto px = level_1<T>(data);
    EXPECT_EQ(size_t, px.size(), 4);
    for (const T value : px) {
        EXPECT(value == expected,
               "mean4(",
               str(in[0]),
               ", ",
               str(in[1]),
               ", ",
               str(in[2]),
               ", ",
               str(in[3]),
               ") = ",
               str(value),
               ", expected ",
               str(expected));
    }
}

/** Two z planes, each uniform, so level 1 is mean2 of the two values. */
template<typename T>
void
check_mean2(const Pair<T>& in, T expected)
{
    zarr::Downsampler downsampler(make_config<T>(true),
                                  ZarrDownsamplingMethod_Mean);
    for (const T value : in) {
        const Quad<T> q{ value, value, value, value };
        auto frame = make_frame<T>({ q, q, q, q });
        downsampler.add_frame(frame);
    }

    std::vector<uint8_t> data;
    CHECK(downsampler.take_frame(1, data));
    const auto px = level_1<T>(data);
    EXPECT_EQ(size_t, px.size(), 4);
    for (const T value : px) {
        EXPECT(value == expected,
               "mean2(",
               str(in[0]),
               ", ",
               str(in[1]),
               ") = ",
               str(value),
               ", expected ",
               str(expected));
    }
}

template<typename T>
void
test_limits()
{
    constexpr T max = std::numeric_limits<T>::max();
    constexpr T min = std::numeric_limits<T>::min();

    check_mean4<T>({ max, max, max, max }, max);
    check_mean4<T>({ max, max, max, T(max - 1) }, T(max - 1));
    check_mean4<T>({ min, min, min, min }, min);
    check_mean2<T>({ max, max }, max);
    check_mean2<T>({ max, T(max - 1) }, T(max - 1));
    check_mean2<T>({ min, min }, min);

    if constexpr (std::is_signed_v<T>) {
        // truncation toward zero, not toward negative infinity
        check_mean4<T>({ min, min, min, T(min + 1) }, T(min + 1));
        check_mean4<T>({ max, min, 0, 0 }, 0);
        check_mean4<T>({ -5, 0, 0, 0 }, -1);
        check_mean4<T>({ -3, 0, 0, 0 }, 0);
        check_mean4<T>({ -1, -1, -1, -2 }, -1);
        check_mean4<T>({ min, T(-1), 1, 0 }, T(min / 4));
        check_mean2<T>({ min, T(min + 1) }, T(min + 1));
        check_mean2<T>({ max, min }, 0);
        check_mean2<T>({ -3, 0 }, -1);

        // mixed signs near the ends, where only the quotient/remainder path
        // of the 64-bit types is exercised
        check_mean4<T>({ max, max, min, 1 }, T(max / 4));
        check_mean4<T>({ T(min + 1), T(min + 1), T(min + 1), T(min + 3) },
                       T(min + 2));
        check_mean4<T>({ max, T(max - 1), -1, -2 }, T(max / 2 - 1));
    } else {
        check_mean4<T>({ max, 1, 0, 0 }, T(T(1) << (8 * sizeof(T) - 2)));
        check_mean2<T>({ max, 1 }, T(T(1) << (8 * sizeof(T) - 1)));
    }
}

/**
 * Random blocks against a wide reference. 64-bit values are kept to 60 bits
 * so that the reference sum in int64_t/uint64_t cannot overflow.
 */
template<typename T>
void
test_random()
{
    using Wide = std::conditional_t<std::is_signed_v<T>, int64_t, uint64_t>;
    constexpr bool is_64 = sizeof(T) == 8;
    const Wide lo = is_64 ? (std::is_signed_v<T> ? -(Wide(1) << 60) : 0)
                          : Wide(std::numeric_limits<T>::min());
    const Wide hi = is_64 ? (Wide(1) << 60) : Wide(std::numeric_limits<T>::max());

    std::mt19937_64 rng(265);
    std::uniform_int_distribution<Wide> dist(lo, hi);

    for (int i = 0; i < 200; ++i) {
        Quad<T> q;
        Wide sum = 0;
        for (auto& v : q) {
            v = static_cast<T>(dist(rng));
            sum += v;
        }
        check_mean4<T>(q, static_cast<T>(sum / 4));

        const Pair<T> p{ q[0], q[1] };
        check_mean2<T>(p, static_cast<T>((Wide(q[0]) + Wide(q[1])) / 2));
    }
}

template<typename T>
void
test_type()
{
    test_limits<T>();
    test_random<T>();
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        test_type<uint8_t>();
        test_type<int8_t>();
        test_type<uint16_t>();
        test_type<int16_t>();
        test_type<uint32_t>();
        test_type<int32_t>();
        test_type<uint64_t>();
        test_type<int64_t>();
        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    return retval;
}
