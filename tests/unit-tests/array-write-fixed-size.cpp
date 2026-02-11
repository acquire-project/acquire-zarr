#include "array.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

namespace {
const std::string store_root = TEST ".zarr";

constexpr unsigned int array_width = 64, array_height = 48, array_planes = 6;
constexpr unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2;
constexpr unsigned int shard_width = 2, shard_height = 1, shard_planes = 1;

std::string
result_to_str(const zarr::WriteResult& result)
{
    switch (result) {
        case zarr::WriteResult::Ok:
            return "OK";
        case zarr::WriteResult::PartialWrite:
            return "Partial write";
        case zarr::WriteResult::OutOfBounds:
            return "Out of bounds";
        case zarr::WriteResult::FrameSizeMismatch:
            return "Frame size mismatch";
        default:
            return "Unknown";
    }
}

void
err_callback(const std::string& err)
{
    LOG_ERROR(err);
}

zarr::Array
make_array()
{
    constexpr auto dtype = ZarrDataType_uint16;
    std::vector<ZarrDimension> dims{
        { "z",
          ZarrDimensionType_Space,
          array_planes,
          chunk_planes,
          shard_planes },
        { "z",
          ZarrDimensionType_Space,
          array_height,
          chunk_height,
          shard_height },
        { "z", ZarrDimensionType_Space, array_width, chunk_width, shard_width },
    };
    auto array_dims = std::make_shared<ArrayDimensions>(std::move(dims), dtype);

    auto config = std::make_shared<zarr::ArrayConfig>(store_root,
                                                      "",
                                                      std::nullopt,
                                                      std::nullopt,
                                                      array_dims,
                                                      dtype,
                                                      std::nullopt,
                                                      0);
    auto thread_pool = std::make_shared<zarr::ThreadPool>(0, err_callback);
    auto fh_pool = std::make_shared<zarr::FileHandlePool>();

    return { config, thread_pool, fh_pool, nullptr };
}

[[nodiscard]] bool
try_append_beyond_bounds(zarr::Array& array)
{
    zarr::LockedBuffer frame(std::move(
      std::vector<uint8_t>(array_height * array_width * sizeof(uint16_t), 1)));

    size_t bytes_out;

    // append to full
    for (auto i = 0; i < array_planes; ++i) {
        EXPECT(array.write_frame(frame, bytes_out) == zarr::WriteResult::Ok,
               "Failed to write frames");
        EXPECT(bytes_out == frame.size(),
               "Expected write of ",
               frame.size(),
               " bytes, wrote ",
               bytes_out);
    }

    // try to append beyond the bounds of the array

    if (const auto result = array.write_frame(frame, bytes_out);
        result != zarr::WriteResult::OutOfBounds) {
        LOG_ERROR("Unexpected write result: ", result_to_str(result));
        return false;
    }

    return true;
}
} // namespace

int
main()
{
    int retval = 1;
    try {
        auto array = make_array();
        retval = !try_append_beyond_bounds(array);
    } catch (const std::exception& exc) {
        LOG_ERROR("Test failed: ", exc.what());
    }

    return retval;
}