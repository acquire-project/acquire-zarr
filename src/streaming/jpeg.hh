#pragma once

#include "zarr.types.h"

#include <nlohmann/json.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace zarr {
struct JpegParams
{
    uint8_t quality{ 0 };
    double window_min{ 0.0 };
    double window_max{ 0.0 };
    ZarrDataType source_dtype{ ZarrDataType_uint8 };
};

/**
 * @brief Map each sample of @p src linearly from [window_min, window_max] to
 * [0, 255], with rounding and clamping. uint8 data is copied unchanged.
 */
std::vector<uint8_t>
convert_to_uint8(std::span<const uint8_t> src,
                 ZarrDataType dtype,
                 const JpegParams& params);

/**
 * @brief Encode @p src, a C-order uint8 grayscale image of @p height rows and
 * @p width columns, as a baseline JPEG with libjpeg-turbo.
 * @return True on success. On failure, @p error holds the reason.
 */
bool
encode_jpeg(std::span<const uint8_t> src,
            size_t width,
            size_t height,
            uint8_t quality,
            std::vector<uint8_t>& out,
            std::string& error);

/**
 * @brief The inner codecs of a JPEG level: `reshape` (for 3 or more
 * dimensions) followed by `jpeg`, as in zarr-extensions PR #66 at fc04227.
 * @param ndims The number of dimensions in the array metadata.
 */
nlohmann::json
jpeg_inner_codecs(size_t ndims, uint8_t quality);
} // namespace zarr
