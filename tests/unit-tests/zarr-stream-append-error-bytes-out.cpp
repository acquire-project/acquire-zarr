#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

/**
 * An append that fails on an earlier stream error must still write bytes_out.
 * A regular file where the shard directory "c" belongs makes the frame-queue
 * thread fail to create the shard directories, which records a stream error;
 * a later append then returns InternalError before it accepts any data.
 */
namespace {
constexpr size_t array_width = 64, array_height = 48;
const std::string data_path =
  (fs::temp_directory_path() / TEST ".zarr").string();

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim++ = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 1,
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
}
} // namespace

int
main()
{
    int retval = 1;

    ZarrStream* stream = nullptr;
    ZarrStreamSettings settings = {};

    Zarr_set_log_level(ZarrLogLevel_Debug);

    std::error_code ec;
    fs::remove_all(data_path, ec);

    settings.store_path = data_path.c_str();
    settings.max_threads = 0;

    ZarrStreamSettings_create_arrays(&settings, 1);
    settings.arrays->data_type = ZarrDataType_uint8;

    try {
        configure_stream_dimensions(settings.arrays);
        stream = ZarrStream_create(&settings);
        CHECK(nullptr != stream);

        {
            std::ofstream blocker(fs::path(data_path) / "c");
            CHECK(blocker.good());
        }

        std::vector<uint8_t> frame(array_width * array_height, 1);
        size_t bytes_out = 0;
        ZarrStatusCode status = ZarrStatusCode_Success;
        for (int i = 0; i < 1000 && status == ZarrStatusCode_Success; ++i) {
            status = ZarrStream_append(
              stream, frame.data(), frame.size(), &bytes_out, nullptr);
            if (status == ZarrStatusCode_Success) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        EXPECT(status != ZarrStatusCode_Success,
               "the shard write never failed");

        bytes_out = 12345;
        status = ZarrStream_append(
          stream, frame.data(), frame.size(), &bytes_out, nullptr);
        EXPECT_EQ(int, status, ZarrStatusCode_InternalError);
        EXPECT_EQ(size_t, bytes_out, 0);

        CHECK(ZarrStream_close(stream) != ZarrStatusCode_Success);
        stream = nullptr;

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    if (stream) {
        ZarrStream_destroy(stream);
    }

    if (settings.arrays) {
        ZarrStreamSettings_destroy_arrays(&settings);
    }

    fs::remove_all(data_path, ec);
    return retval;
}
