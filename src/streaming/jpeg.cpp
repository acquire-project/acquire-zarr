#include "jpeg.hh"
#include "macros.hh"

#include <turbojpeg.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <type_traits>

namespace {
/**
 * @brief Map each sample as the codec chain in the metadata describes:
 * cast_value to F, scale_offset in F, then cast_value to uint8 with
 * nearest-even rounding and clamping. Computing in F, with the offset and
 * scale rounded to F, makes the stored bytes what a reader of the chain
 * expects.
 */
template<typename T, typename F>
void
convert(std::span<const uint8_t> src,
        std::vector<uint8_t>& dst,
        const zarr::JpegScaleOffset& so)
{
    const auto n = src.size() / sizeof(T);
    EXPECT(n * sizeof(T) == src.size(),
           "Frame size ",
           src.size(),
           " is not a multiple of the sample size ",
           sizeof(T));
    dst.resize(n);

    const auto* in = reinterpret_cast<const T*>(src.data());
    const auto offset = static_cast<F>(so.offset);
    const auto scale = static_cast<F>(so.scale);
    for (size_t i = 0; i < n; ++i) {
        // cast_value must fail on a value that uint8 cannot represent and no
        // scalar_map covers, as a division by zero would
        if constexpr (std::is_floating_point_v<T>) {
            EXPECT(std::isfinite(in[i]),
                   "Cannot map sample ",
                   i,
                   " (",
                   in[i],
                   ") of a JPEG level to uint8: only finite values are "
                   "supported");
        }
        const F v = std::nearbyint((static_cast<F>(in[i]) - offset) * scale);
        dst[i] = v >= F(255) ? 255 : (v > F(0) ? static_cast<uint8_t>(v) : 0);
    }
}

template<typename T>
void
convert(std::span<const uint8_t> src,
        std::vector<uint8_t>& dst,
        ZarrDataType intermediate,
        const zarr::JpegScaleOffset& so)
{
    if (intermediate == ZarrDataType_float32) {
        convert<T, float>(src, dst, so);
    } else {
        convert<T, double>(src, dst, so);
    }
}

struct TjDeleter
{
    void operator()(void* handle) const { tj3Destroy(handle); }
};
} // namespace

ZarrDataType
zarr::jpeg_intermediate_type(ZarrDataType source)
{
    switch (source) {
        case ZarrDataType_uint8:
        case ZarrDataType_int8:
        case ZarrDataType_uint16:
        case ZarrDataType_int16:
        case ZarrDataType_float32:
            return ZarrDataType_float32; // exact for these sources
        default:
            return ZarrDataType_float64;
    }
}

zarr::JpegScaleOffset
zarr::jpeg_scale_offset(const JpegParams& params)
{
    const double offset = params.window_min;
    const double scale = 255.0 / (params.window_max - params.window_min);
    if (jpeg_intermediate_type(params.source_dtype) == ZarrDataType_float32) {
        return { static_cast<float>(offset), static_cast<float>(scale) };
    }
    return { offset, scale };
}

std::vector<uint8_t>
zarr::convert_to_uint8(std::span<const uint8_t> src, const JpegParams& params)
{
    std::vector<uint8_t> dst;
    const auto dtype = params.source_dtype;
    const auto f = jpeg_intermediate_type(dtype);
    const auto so = jpeg_scale_offset(params);

    switch (dtype) {
        case ZarrDataType_uint8:
            dst.assign(src.begin(), src.end());
            break;
        case ZarrDataType_uint16:
            convert<uint16_t>(src, dst, f, so);
            break;
        case ZarrDataType_uint32:
            convert<uint32_t>(src, dst, f, so);
            break;
        case ZarrDataType_uint64:
            convert<uint64_t>(src, dst, f, so);
            break;
        case ZarrDataType_int8:
            convert<int8_t>(src, dst, f, so);
            break;
        case ZarrDataType_int16:
            convert<int16_t>(src, dst, f, so);
            break;
        case ZarrDataType_int32:
            convert<int32_t>(src, dst, f, so);
            break;
        case ZarrDataType_int64:
            convert<int64_t>(src, dst, f, so);
            break;
        case ZarrDataType_float32:
            convert<float>(src, dst, f, so);
            break;
        case ZarrDataType_float64:
            convert<double>(src, dst, f, so);
            break;
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(dtype));
    }

    return dst;
}

bool
zarr::encode_jpeg(std::span<const uint8_t> src,
                  size_t width,
                  size_t height,
                  uint8_t quality,
                  std::vector<uint8_t>& out,
                  std::string& error)
{
    if (src.size() != width * height) {
        error = "JPEG input has " + std::to_string(src.size()) +
                " bytes; expected " + std::to_string(width) + " x " +
                std::to_string(height);
        return false;
    }

    // a TurboJPEG handle is not safe to share between threads
    thread_local std::unique_ptr<void, TjDeleter> handle(
      tj3Init(TJINIT_COMPRESS));
    if (!handle) {
        error = "Failed to create a TurboJPEG compressor";
        return false;
    }

    auto* h = handle.get();
    if (tj3Set(h, TJPARAM_QUALITY, quality) != 0 ||
        tj3Set(h, TJPARAM_SUBSAMP, TJSAMP_GRAY) != 0 ||
        tj3Set(h, TJPARAM_PROGRESSIVE, 0) != 0 ||
        tj3Set(h, TJPARAM_ARITHMETIC, 0) != 0 ||
        tj3Set(h, TJPARAM_LOSSLESS, 0) != 0) {
        error =
          std::string("Failed to configure TurboJPEG: ") + tj3GetErrorStr(h);
        return false;
    }

    unsigned char* buf = nullptr;
    size_t size = 0;
    if (tj3Compress8(h,
                     src.data(),
                     static_cast<int>(width),
                     0, // pitch: rows are contiguous
                     static_cast<int>(height),
                     TJPF_GRAY,
                     &buf,
                     &size) != 0) {
        error = std::string("JPEG encoding failed: ") + tj3GetErrorStr(h);
        tj3Free(buf);
        return false;
    }

    out.assign(buf, buf + size);
    tj3Free(buf);
    return true;
}

nlohmann::json
zarr::jpeg_inner_codecs(size_t ndims, const JpegParams& params)
{
    auto codecs = nlohmann::json::array();

    // the level keeps the data type of level 0, so that a viewer sees one data
    // type across the pyramid; the chain maps it to the uint8 that JPEG needs
    if (params.source_dtype != ZarrDataType_uint8) {
        const auto f = jpeg_intermediate_type(params.source_dtype);
        const auto so = jpeg_scale_offset(params);
        if (f != params.source_dtype) {
            codecs.push_back({
              { "name", "cast_value" },
              { "configuration",
                { { "data_type",
                    f == ZarrDataType_float32 ? "float32" : "float64" } } },
            });
        }
        codecs.push_back({
          { "name", "scale_offset" },
          { "configuration",
            { { "offset", so.offset }, { "scale", so.scale } } },
        });
        codecs.push_back({
          { "name", "cast_value" },
          { "configuration",
            {
              { "data_type", "uint8" },
              { "rounding", "nearest-even" },
              { "out_of_range", "clamp" },
            } },
        });
    }

    // a 2D chunk is already an (H, W) image; an identity reshape would only
    // force readers to implement the reshape codec
    if (ndims > 2) {
        auto leading = nlohmann::json::array();
        for (size_t i = 0; i + 1 < ndims; ++i) {
            leading.push_back(i);
        }
        codecs.push_back({
          { "name", "reshape" },
          { "configuration",
            { { "shape",
                nlohmann::json::array({ leading, { ndims - 1 } }) } } },
        });
    }

    codecs.push_back({
      { "name", "jpeg" },
      { "configuration",
        {
          { "quality", params.quality },
          { "encoded_color_space", "grayscale" },
          { "subsampling", { { 1, 1 } } },
        } },
    });

    return codecs;
}
