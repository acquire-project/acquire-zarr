#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

/**
 * ZarrStream_close must tell an error recorded during streaming
 * (ZarrStatusCode_InternalError) from a failure while it finalizes the arrays
 * (ZarrStatusCode_IOError). A regular file where the shard directory "c"
 * belongs makes every shard write fail. Placed before a full chunk is
 * appended, the first flush fails on a worker during streaming; placed after
 * only a partial chunk is appended, the first flush is the one at close.
 */
namespace {
constexpr size_t array_width = 64, array_height = 48;
constexpr size_t frame_bytes = array_width * array_height;
constexpr uint32_t frames_per_chunk = 10;

const std::string data_path =
  (fs::temp_directory_path() / TEST ".zarr").string();

ZarrStream*
make_stream(ZarrStreamSettings& settings)
{
    std::error_code ec;
    fs::remove_all(data_path, ec);

    settings = {};
    settings.store_path = data_path.c_str();
    settings.max_threads = 0;

    CHECK(ZarrStreamSettings_create_arrays(&settings, 1) ==
          ZarrStatusCode_Success);
    settings.arrays->data_type = ZarrDataType_uint8;
    CHECK(ZarrArraySettings_create_dimension_array(settings.arrays, 3) ==
          ZarrStatusCode_Success);

    ZarrDimensionProperties* dim = settings.arrays->dimensions;
    *dim++ = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = frames_per_chunk,
        .shard_size_chunks = 1,
    };
    *dim++ = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_height,
        .chunk_size_px = array_height,
        .shard_size_chunks = 1,
    };
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_width,
        .chunk_size_px = array_width,
        .shard_size_chunks = 1,
    };

    ZarrStream* stream = ZarrStream_create(&settings);
    CHECK(stream != nullptr);
    return stream;
}

void
block_shard_directory()
{
    std::ofstream blocker(fs::path(data_path) / "c");
    CHECK(blocker.good());
}

ZarrStatusCode
append_frame(ZarrStream* stream)
{
    static const std::vector<uint8_t> frame(frame_bytes, 7);
    size_t bytes_out = 0;
    return ZarrStream_append(
      stream, frame.data(), frame.size(), &bytes_out, nullptr);
}

void
test_streaming_error_is_internal_error()
{
    ZarrStreamSettings settings;
    ZarrStream* stream = make_stream(settings);
    block_shard_directory();

    // keep appending until the stream reports the failed flush, so that the
    // error is recorded before close begins
    ZarrStatusCode status = ZarrStatusCode_Success;
    for (int i = 0; i < 2000 && status == ZarrStatusCode_Success; ++i) {
        status = append_frame(stream);
        if (status == ZarrStatusCode_Success && i >= frames_per_chunk) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    EXPECT(status != ZarrStatusCode_Success, "the shard write never failed");

    EXPECT_EQ(int, ZarrStream_close(stream), ZarrStatusCode_InternalError);
    ZarrStreamSettings_destroy_arrays(&settings);
}

void
test_finalize_error_is_io_error()
{
    ZarrStreamSettings settings;
    ZarrStream* stream = make_stream(settings);

    // less than one chunk: nothing is flushed until close
    for (uint32_t i = 0; i < frames_per_chunk / 2; ++i) {
        EXPECT_EQ(int, append_frame(stream), ZarrStatusCode_Success);
    }
    block_shard_directory();

    EXPECT_EQ(int, ZarrStream_close(stream), ZarrStatusCode_IOError);
    ZarrStreamSettings_destroy_arrays(&settings);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        test_streaming_error_is_internal_error();
        test_finalize_error_is_io_error();
        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    std::error_code ec;
    fs::remove_all(data_path, ec);
    return retval;
}
