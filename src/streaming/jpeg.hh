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

struct JpegScaleOffset
{
    double offset;
    double scale;
};

/**
 * @brief The floating-point type in which the window is applied: float32
 * where it represents every source value exactly (8- and 16-bit integers and
 * float32), float64 otherwise. 64-bit integers round to float64 with
 * nearest-even, the cast_value default.
 */
ZarrDataType
jpeg_intermediate_type(ZarrDataType source);

/**
 * @brief The `scale_offset` parameters that map [window_min, window_max] to
 * [0, 255], rounded to the intermediate type.
 */
JpegScaleOffset
jpeg_scale_offset(const JpegParams& params);

/**
 * @brief The fill value of a JPEG level: window_min, which the codec chain maps
 * to 0 and decodes back exactly, as cast_value requires of the fill value.
 * Validation makes window_min representable in the level's data type.
 */
nlohmann::json
jpeg_fill_value(const JpegParams& params);

/**
 * @brief Map each sample of @p src, of type @p params.source_dtype, to uint8
 * as the codec chain from jpeg_inner_codecs() describes. uint8 data is copied
 * unchanged.
 * @throws std::runtime_error on a NaN or infinite sample, or one that overflows
 * the window arithmetic.
 */
std::vector<uint8_t>
convert_to_uint8(std::span<const uint8_t> src, const JpegParams& params);

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
 * @brief The inner codecs of a JPEG level: for data other than uint8,
 * `cast_value`, `scale_offset` and `cast_value` to uint8; then `reshape` (for
 * 3 or more dimensions); then `jpeg`, as specified in zarr-extensions
 * (codecs/jpeg, merged as 96a2509). `subsampling` is omitted: the spec says
 * so for grayscale, whose default is [[1, 1]].
 * @param ndims The number of dimensions in the array metadata.
 */
nlohmann::json
jpeg_inner_codecs(size_t ndims, const JpegParams& params);
} // namespace zarr
