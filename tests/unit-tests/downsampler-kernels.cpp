// Pin the output of the 2x2 and two-frame reductions for every data type and
// method, on odd and even frame sizes, against literal values and a scalar
// reference.
#include "downsampler.hh"
#include "unit.test.macros.hh"

#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <type_traits>
#include <vector>

namespace {
template<typename T>
ZarrDataType
dtype_of()
{
    if constexpr (std::is_same_v<T, uint8_t>)
        return ZarrDataType_uint8;
    if constexpr (std::is_same_v<T, int8_t>)
        return ZarrDataType_int8;
    if constexpr (std::is_same_v<T, uint16_t>)
        return ZarrDataType_uint16;
    if constexpr (std::is_same_v<T, int16_t>)
        return ZarrDataType_int16;
    if constexpr (std::is_same_v<T, uint32_t>)
        return ZarrDataType_uint32;
    if constexpr (std::is_same_v<T, int32_t>)
        return ZarrDataType_int32;
    if constexpr (std::is_same_v<T, uint64_t>)
        return ZarrDataType_uint64;
    if constexpr (std::is_same_v<T, int64_t>)
        return ZarrDataType_int64;
    if constexpr (std::is_same_v<T, float>)
        return ZarrDataType_float32;
    if constexpr (std::is_same_v<T, double>)
        return ZarrDataType_float64;
}

// mean truncated toward zero, without overflow
template<typename T>
T
ref_mean(std::initializer_list<T> values)
{
    if constexpr (std::is_floating_point_v<T>) {
        T sum = 0;
        for (T v : values)
            sum += v;
        return sum / static_cast<T>(values.size());
    } else {
        // 128-bit integers are exact here; integer division truncates toward
        // zero
        __int128 sum = 0;
        for (T v : values)
            sum += v;
        return static_cast<T>(sum / static_cast<__int128>(values.size()));
    }
}

template<typename T>
T
ref_reduce(ZarrDownsamplingMethod m, std::initializer_list<T> v)
{
    switch (m) {
        case ZarrDownsamplingMethod_Decimate:
            return *v.begin();
        case ZarrDownsamplingMethod_Mean:
            return ref_mean<T>(v);
        case ZarrDownsamplingMethod_Min: {
            T r = *v.begin();
            for (T x : v)
                r = x < r ? x : r;
            return r;
        }
        default: {
            T r = *v.begin();
            for (T x : v)
                r = x > r ? x : r;
            return r;
        }
    }
}

// 2x2 reduction; an odd final row or column pairs with itself
template<typename T>
std::vector<T>
ref_scale(const std::vector<T>& src,
          size_t w,
          size_t h,
          ZarrDownsamplingMethod m)
{
    std::vector<T> dst;
    for (size_t r = 0; r < h; r += 2) {
        const size_t r2 = r + 1 < h ? r + 1 : r;
        for (size_t c = 0; c < w; c += 2) {
            const size_t c2 = c + 1 < w ? c + 1 : c;
            dst.push_back(ref_reduce<T>(m,
                                        { src[r * w + c],
                                          src[r * w + c2],
                                          src[r2 * w + c],
                                          src[r2 * w + c2] }));
        }
    }
    return dst;
}

// level 1 of a z-spatial array with 2 planes of h x w
template<typename T>
std::vector<T>
downsample(const std::vector<T>& plane0,
           const std::vector<T>& plane1,
           uint32_t w,
           uint32_t h,
           ZarrDownsamplingMethod m)
{
    auto dims = std::make_shared<ArrayDimensions>(
      std::vector<ZarrDimension>{ { "z", ZarrDimensionType_Space, 2, 1, 1 },
                                  { "y", ZarrDimensionType_Space, h, 2, 1 },
                                  { "x", ZarrDimensionType_Space, w, 2, 1 } },
      dtype_of<T>());
    auto config = std::make_shared<zarr::ArrayConfig>(
      "", "/0", std::nullopt, std::nullopt, dims, dtype_of<T>(), m, 0, true);
    zarr::Downsampler ds(config, m);

    for (const auto* plane : { &plane0, &plane1 }) {
        std::vector<uint8_t> bytes(plane->size() * sizeof(T));
        std::memcpy(bytes.data(), plane->data(), bytes.size());
        ds.add_frame(bytes);
    }

    std::vector<uint8_t> out;
    EXPECT(ds.take_frame(1, out), "No level-1 frame");
    std::vector<T> result(out.size() / sizeof(T));
    std::memcpy(result.data(), out.data(), out.size());
    return result;
}

const ZarrDownsamplingMethod kMethods[] = { ZarrDownsamplingMethod_Decimate,
                                            ZarrDownsamplingMethod_Mean,
                                            ZarrDownsamplingMethod_Min,
                                            ZarrDownsamplingMethod_Max };

template<typename T>
bool
same(T a, T b)
{
    if constexpr (std::is_floating_point_v<T>) {
        return a == b || (std::isnan(a) && std::isnan(b));
    } else {
        return a == b;
    }
}

template<typename T>
void
test_against_reference()
{
    std::mt19937_64 rng(42);
    const T lo = std::numeric_limits<T>::lowest();
    const T hi = std::numeric_limits<T>::max();

    for (const auto [w, h] : { std::pair{ 5u, 3u }, { 6u, 4u }, { 7u, 9u } }) {
        std::vector<T> p0(w * h), p1(w * h);
        for (size_t i = 0; i < p0.size(); ++i) {
            if constexpr (std::is_floating_point_v<T>) {
                p0[i] =
                  static_cast<T>(std::normal_distribution<>(0, 1000)(rng));
                p1[i] =
                  static_cast<T>(std::normal_distribution<>(0, 1000)(rng));
            } else {
                p0[i] = static_cast<T>(rng());
                p1[i] = static_cast<T>(rng());
            }
        }
        p0[0] = hi; // extremes exercise overflow and truncation
        p0[1] = hi;
        p0[w] = lo;
        p1[w + 1] = lo;

        for (const auto m : kMethods) {
            const auto got = downsample(p0, p1, w, h, m);
            const auto s0 = ref_scale(p0, w, h, m), s1 = ref_scale(p1, w, h, m);
            EXPECT_EQ(size_t, got.size(), s0.size());
            for (size_t i = 0; i < got.size(); ++i) {
                const T want = ref_reduce<T>(m, { s0[i], s1[i] });
                EXPECT(same(got[i], want),
                       "dtype ",
                       dtype_of<T>(),
                       " method ",
                       m,
                       " size ",
                       w,
                       "x",
                       h,
                       " pixel ",
                       i,
                       ": got ",
                       +got[i],
                       ", want ",
                       +want);
            }
        }
    }
}

void
test_literal_int16()
{
    // 3 rows x 5 columns: the last row and column pair with themselves
    const std::vector<int16_t> p = { -3, 5,  7,  -1, 9,  //
                                     4,  -6, 2,  8,  -2, //
                                     1,  0,  -7, 3,  6 };
    // 2x2 blocks: {-3,5,4,-6} {7,-1,2,8} {9,9,-2,-2}
    //             {1,0,1,0}   {-7,3,-7,3} {6,6,6,6}
    const std::vector<int16_t> mean = { 0, 4, 3, 0, -2, 6 };
    const std::vector<int16_t> min = { -6, -1, -2, 0, -7, 6 };
    const std::vector<int16_t> max = { 5, 8, 9, 1, 3, 6 };
    const std::vector<int16_t> dec = { -3, 7, 9, 1, -7, 6 };

    // with identical planes, the z combine leaves the 2x2 result unchanged
    EXPECT(downsample(p, p, 5, 3, ZarrDownsamplingMethod_Mean) == mean,
           "int16 mean");
    EXPECT(downsample(p, p, 5, 3, ZarrDownsamplingMethod_Min) == min,
           "int16 min");
    EXPECT(downsample(p, p, 5, 3, ZarrDownsamplingMethod_Max) == max,
           "int16 max");
    EXPECT(downsample(p, p, 5, 3, ZarrDownsamplingMethod_Decimate) == dec,
           "int16 decimate");
}
} // namespace

int
main()
{
    int retval = 1;
    try {
        test_literal_int16();
        test_against_reference<uint8_t>();
        test_against_reference<int8_t>();
        test_against_reference<uint16_t>();
        test_against_reference<int16_t>();
        test_against_reference<uint32_t>();
        test_against_reference<int32_t>();
        test_against_reference<uint64_t>();
        test_against_reference<int64_t>();
        test_against_reference<float>();
        test_against_reference<double>();
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }
    return retval;
}
