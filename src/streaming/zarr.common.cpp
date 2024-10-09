#include "macros.hh"
#include "zarr.common.hh"

#include <stdexcept>
#include <algorithm>

std::string
zarr::trim(std::string_view s)
{
    if (s.empty()) {
        return {};
    }

    // trim left
    std::string trimmed(s);
    trimmed.erase(trimmed.begin(),
                  std::find_if(trimmed.begin(), trimmed.end(), [](char c) {
                      return !std::isspace(c);
                  }));

    // trim right
    trimmed.erase(std::find_if(trimmed.rbegin(),
                               trimmed.rend(),
                               [](char c) { return !std::isspace(c); })
                    .base(),
                  trimmed.end());

    return trimmed;
}

bool
zarr::is_empty_string(std::string_view s, std::string_view err_on_empty)
{
    auto trimmed = trim(s);
    if (trimmed.empty()) {
        LOG_ERROR(err_on_empty);
        return true;
    }
    return false;
}

size_t
zarr::bytes_of_type(ZarrDataType data_type)
{
    switch (data_type) {
        case ZarrDataType_int8:
        case ZarrDataType_uint8:
            return 1;
        case ZarrDataType_int16:
        case ZarrDataType_uint16:
            return 2;
        case ZarrDataType_int32:
        case ZarrDataType_uint32:
        case ZarrDataType_float32:
            return 4;
        case ZarrDataType_int64:
        case ZarrDataType_uint64:
        case ZarrDataType_float64:
            return 8;
        default:
            throw std::invalid_argument("Invalid data type: " +
                                        std::to_string(data_type));
    }
}

size_t
zarr::bytes_of_frame(const ArrayDimensions& dims, ZarrDataType type)
{
    const auto height = dims.height_dim().array_size_px;
    const auto width = dims.width_dim().array_size_px;
    return bytes_of_type(type) * height * width;
}

uint32_t
zarr::chunks_along_dimension(const ZarrDimension& dimension)
{
    EXPECT(dimension.chunk_size_px > 0, "Invalid chunk size.");

    return (dimension.array_size_px + dimension.chunk_size_px - 1) /
           dimension.chunk_size_px;
}

uint32_t
zarr::shards_along_dimension(const ZarrDimension& dimension)
{
    if (dimension.shard_size_chunks == 0) {
        return 0;
    }

    const auto shard_size = dimension.shard_size_chunks;
    const auto n_chunks = chunks_along_dimension(dimension);
    return (n_chunks + shard_size - 1) / shard_size;
}
