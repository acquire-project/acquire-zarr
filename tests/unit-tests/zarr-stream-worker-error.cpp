#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <filesystem>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

/**
 * A shard write that fails on a pool worker records the stream error on that
 * worker, while the caller keeps appending on its own thread. A directory where
 * the first shard file belongs makes that write fail. The caller must see the
 * error from a later append, and close must fail. The failure path needs a pool
 * worker, so the test does nothing on a host with one hardware thread, where the
 * pool has one thread and chunk jobs run inline.
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
    if (std::thread::hardware_concurrency() < 2) {
        LOG_WARNING("Skipping: the pool has no worker on a single-core host");
        return 0;
    }

    int retval = 1;

    ZarrStream* stream = nullptr;
    ZarrStreamSettings settings = {};

    Zarr_set_log_level(ZarrLogLevel_Error);

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

        // the first shard of a 3D array is c/0/0/0
        fs::create_directories(fs::path(data_path) / "c" / "0" / "0" / "0");

        std::vector<uint8_t> frame(array_width * array_height, 1);
        size_t bytes_out = 0;
        ZarrStatusCode status = ZarrStatusCode_Success;
        for (int i = 0; i < 2000 && status == ZarrStatusCode_Success; ++i) {
            status = ZarrStream_append(
              stream, frame.data(), frame.size(), &bytes_out, nullptr);
        }
        EXPECT(status != ZarrStatusCode_Success,
               "the shard write never failed");
        EXPECT_EQ(int, status, ZarrStatusCode_InternalError);

        status = ZarrStream_close(stream);
        stream = nullptr;
        CHECK(status != ZarrStatusCode_Success);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    if (stream != nullptr) {
        ZarrStream_destroy(stream);
    }
    ZarrStreamSettings_destroy_arrays(&settings);
    fs::remove_all(data_path, ec);

    return retval;
}
