// Mirrors python/tests/test_stream.py::test_append_throws_on_overflow.
// Fixed-size array on the append dimension; first append fills it exactly,
// second append of extra data must return WriteOutOfBounds.

#include "acquire.zarr.h"
#include "test.macros.hh"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const size_t array_z = 6;  // fixed size on append dim
const size_t array_y = 48;
const size_t array_x = 64;
const fs::path test_path = "throws-on-overflow-test.zarr";

ZarrStream*
setup()
{
    const auto test_path_str = test_path.string();

    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    ZarrArraySettings array = { .data_type = ZarrDataType_uint16 };
    ZarrStreamSettings settings = {
        .store_path = test_path_str.c_str(),
        .arrays = &array,
        .array_count = 1,
    };

    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings.arrays, 3));

    settings.arrays->dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_z,
        .chunk_size_px = 2,
        .shard_size_chunks = 1,
    };
    settings.arrays->dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_y,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };
    settings.arrays->dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_x,
        .chunk_size_px = 16,
        .shard_size_chunks = 2,
    };

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);
    CHECK(stream != nullptr);
    return stream;
}
} // namespace

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Error);

    auto* stream = setup();
    if (!stream) {
        LOG_ERROR("Failed to create ZarrStream");
        return 1;
    }

    int retval = 1;

    try {
        const size_t full_bytes = array_z * array_y * array_x * sizeof(uint16_t);
        std::vector<uint16_t> full(array_z * array_y * array_x, 0);

        size_t bytes_out = 0;
        ZarrStatusCode st = ZarrStream_append(
          stream, full.data(), full_bytes, &bytes_out, nullptr);
        EXPECT(st == ZarrStatusCode_Success,
               "First append failed with status ", st);
        EXPECT_EQ(size_t, bytes_out, full_bytes);

        uint16_t one = 0;
        bytes_out = 0;
        st = ZarrStream_append(
          stream, &one, sizeof(one), &bytes_out, nullptr);
        EXPECT(st == ZarrStatusCode_WriteOutOfBounds,
               "Expected WriteOutOfBounds on overflow append, got ", st);

        ZarrStream_destroy(stream);
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }
    return retval;
}
