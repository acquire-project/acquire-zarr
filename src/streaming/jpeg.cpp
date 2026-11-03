#include "jpeg.hh"
#include "macros.hh"

#include <turbojpeg.h>

#include <algorithm>
#include <cmath>
#include <memory>

namespace {
template<typename T>
void
convert(std::span<const uint8_t> src,
        std::vector<uint8_t>& dst,
        double lo,
        double hi)
{
    const auto n = src.size() / sizeof(T);
    EXPECT(n * sizeof(T) == src.size(),
           "Frame size ",
           src.size(),
           " is not a multiple of the sample size ",
           sizeof(T));
    dst.resize(n);

    const auto* in = reinterpret_cast<const T*>(src.data());
    const double scale = 255.0 / (hi - lo);
    for (size_t i = 0; i < n; ++i) {
        const double v = (static_cast<double>(in[i]) - lo) * scale;
        dst[i] = static_cast<uint8_t>(
          std::clamp(std::nearbyint(v), 0.0, 255.0)); // NaN maps to 0
    }
}

struct TjDeleter
{
    void operator()(void* handle) const { tj3Destroy(handle); }
};
} // namespace

std::vector<uint8_t>
zarr::convert_to_uint8(std::span<const uint8_t> src,
                       ZarrDataType dtype,
                       const JpegParams& params)
{
    std::vector<uint8_t> dst;
    const auto lo = params.window_min, hi = params.window_max;

    switch (dtype) {
        case ZarrDataType_uint8:
            dst.assign(src.begin(), src.end());
            break;
        case ZarrDataType_uint16:
            convert<uint16_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_uint32:
            convert<uint32_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_uint64:
            convert<uint64_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_int8:
            convert<int8_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_int16:
            convert<int16_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_int32:
            convert<int32_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_int64:
            convert<int64_t>(src, dst, lo, hi);
            break;
        case ZarrDataType_float32:
            convert<float>(src, dst, lo, hi);
            break;
        case ZarrDataType_float64:
            convert<double>(src, dst, lo, hi);
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
zarr::jpeg_inner_codecs(size_t ndims, uint8_t quality)
{
    auto codecs = nlohmann::json::array();

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
          { "quality", quality },
          { "encoded_color_space", "grayscale" },
          { "subsampling", { { 1, 1 } } },
        } },
    });

    return codecs;
}
