#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 1,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 48,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
    };
}

void
verify_file_data(const ZarrStreamSettings& settings)
{
    std::vector<uint8_t> buffer;
    const size_t row_size = settings.arrays->dimensions[2].array_size_px,
                 num_rows = settings.arrays->dimensions[1].array_size_px;

    fs::path shard_path =
      fs::path(settings.store_path) / "0" / "c" / "0" / "0" / "0";
    CHECK(fs::is_regular_file(shard_path));

    // Open and read the first chunk file
    {
        std::ifstream file(shard_path, std::ios::binary);
        CHECK(file.is_open());

        // Get file size
        file.seekg(0, std::ios::end);
        const auto file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        // Read entire file into buffer
        buffer.resize(file_size);
        file.read(reinterpret_cast<char*>(buffer.data()), file_size);
        CHECK(file.good());
    }

    // Verify each row contains the correct values
    constexpr size_t table_size = 2 * sizeof(uint64_t) + 4;
    EXPECT_EQ(int, buffer.size(), row_size* num_rows + table_size);
    for (size_t row = 0; row < num_rows; ++row) {
        // Check each byte in this row
        for (size_t col = 0; col < row_size; ++col) {
            const size_t index = row * row_size + col;
            EXPECT_EQ(int, buffer[index], row);
        }
    }

    shard_path = fs::path(settings.store_path) / "0" / "c" / "1" / "0" / "0";
    CHECK(fs::is_regular_file(shard_path));

    // Open and read the next chunk file
    {
        std::ifstream file(shard_path, std::ios::binary);
        CHECK(file.is_open());

        // Get file size
        file.seekg(0, std::ios::end);
        const auto file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        // Read entire file into buffer
        buffer.resize(file_size);
        file.read(reinterpret_cast<char*>(buffer.data()), file_size);
        CHECK(file.good());
    }

    // Verify each row contains the correct values
    EXPECT_EQ(int, buffer.size(), row_size* num_rows + table_size);
    for (size_t row = 0; row < num_rows; ++row) {
        // Check each byte in this row
        for (size_t col = 0; col < row_size; ++col) {
            const size_t index = row * row_size + col;
            EXPECT_EQ(int, buffer[index], 48 + row);
        }
    }

    // after this, we wrote more bytes than a single frame, in a sequence
    // beginning at 96 and incrementing 1 at a time, so we should have 2 frames
    // starting at 96 and ending at 191
    uint8_t px_value = 96;

    shard_path = fs::path(settings.store_path) / "0" / "c" / "2" / "0" / "0";
    CHECK(fs::is_regular_file(shard_path));

    // Open and read the next chunk file
    {
        std::ifstream file(shard_path, std::ios::binary);
        CHECK(file.is_open());

        // Get file size
        file.seekg(0, std::ios::end);
        const auto file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        // Read entire file into buffer
        buffer.resize(file_size);
        file.read(reinterpret_cast<char*>(buffer.data()), file_size);
        CHECK(file.good());
    }

    // Verify each row contains the correct values
    EXPECT_EQ(int, buffer.size(), row_size* num_rows + table_size);

    for (auto i = 0; i < row_size * num_rows; ++i) {
        EXPECT_EQ(int, buffer[i], px_value++);
    }

    shard_path = fs::path(settings.store_path) / "0" / "c" / "3" / "0" / "0";
    CHECK(fs::is_regular_file(shard_path));

    // Open and read the next chunk file
    {
        std::ifstream file(shard_path, std::ios::binary);
        CHECK(file.is_open());

        // Get file size
        file.seekg(0, std::ios::end);
        const auto file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        // Read entire file into buffer
        buffer.resize(file_size);
        file.read(reinterpret_cast<char*>(buffer.data()), file_size);
        CHECK(file.good());
    }

    // Verify each row contains the correct values
    EXPECT_EQ(int, buffer.size(), row_size* num_rows + table_size);

    for (auto i = 0; i < row_size * num_rows; ++i) {
        EXPECT_EQ(int, buffer[i], px_value++);
    }
}

int
main()
{
    int retval = 1;

    ZarrStream* stream = nullptr;
    ZarrStreamSettings settings = {};

    Zarr_set_log_level(ZarrLogLevel_Debug);

    settings.store_path = static_cast<const char*>(TEST ".zarr");
    settings.max_threads = 0;

    ZarrStreamSettings_create_arrays(&settings, 1);
    settings.arrays->data_type = ZarrDataType_uint8;

    try {
        // allocate dimensions
        configure_stream_dimensions(settings.arrays);
        stream = ZarrStream_create(&settings);

        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings.store_path));

        // append partial frames
        std::vector<uint8_t> data(16, 0);
        for (auto row = 0; row < 96; ++row) { // 2 frames worth of data
            std::fill(data.begin(), data.end(), row);
            for (auto col_group = 0; col_group < 4; ++col_group) {
                const auto bytes_written =
                  stream->append(nullptr, data.data(), data.size());
                EXPECT_EQ(int, data.size(), bytes_written);
            }
        }

        data.resize(2 * 48 * 64);
        std::iota(data.begin(), data.end(), 96);

        // append more than one frame, then fill in the rest
        const auto bytes_to_write = 48 * 64 + 7;
        auto bytes_written =
          stream->append(nullptr, data.data(), bytes_to_write);
        EXPECT_EQ(int, bytes_to_write, bytes_written);

        bytes_written = stream->append(
          nullptr, data.data() + bytes_to_write, data.size() - bytes_to_write);
        EXPECT_EQ(int, data.size() - bytes_to_write, bytes_written);

        // cleanup
        ZarrStream_destroy(stream);
        stream = nullptr;

        verify_file_data(settings);
        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    // cleanup
    if (stream) {
        ZarrStream_destroy(stream);
    }

    if (settings.arrays) {
        ZarrStreamSettings_destroy_arrays(&settings);
    }

    std::error_code ec;
    if (fs::is_directory(settings.store_path) &&
        !fs::remove_all(settings.store_path, ec)) {
        LOG_ERROR("Failed to remove store path: ", ec.message().c_str());
    }
    return retval;
}