#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

void
configure_stream_dimensions(ZarrStreamSettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrStreamSettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 1,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 48,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 64,
    };
}

void
verify_file_data(const ZarrStreamSettings& settings)
{
    std::vector<uint8_t> buffer;
    const size_t row_size = settings.dimensions[2].array_size_px,
                 num_rows = settings.dimensions[1].array_size_px;

    fs::path chunk_path = fs::path(settings.store_path) / "0" / "0" / "0" / "0";
    CHECK(fs::is_regular_file(chunk_path));

    // Open and read the first chunk file
    {
        std::ifstream file(chunk_path, std::ios::binary);
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
    EXPECT_EQ(int, buffer.size(), row_size* num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        // Check each byte in this row
        for (size_t col = 0; col < row_size; ++col) {
            const size_t index = row * row_size + col;
            EXPECT_EQ(int, buffer[index], row);
        }
    }

    chunk_path = fs::path(settings.store_path) / "0" / "1" / "0" / "0";
    CHECK(fs::is_regular_file(chunk_path));

    // Open and read the next chunk file
    {
        std::ifstream file(chunk_path, std::ios::binary);
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
    EXPECT_EQ(int, buffer.size(), row_size* num_rows);
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

    chunk_path = fs::path(settings.store_path) / "0" / "2" / "0" / "0";
    CHECK(fs::is_regular_file(chunk_path));

    // Open and read the next chunk file
    {
        std::ifstream file(chunk_path, std::ios::binary);
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
    EXPECT_EQ(int, buffer.size(), row_size* num_rows);

    for (auto i = 0; i < buffer.size(); ++i) {
        EXPECT_EQ(int, buffer[i], px_value++);
    }

    chunk_path = fs::path(settings.store_path) / "0" / "3" / "0" / "0";
    CHECK(fs::is_regular_file(chunk_path));

    // Open and read the next chunk file
    {
        std::ifstream file(chunk_path, std::ios::binary);
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
    EXPECT_EQ(int, buffer.size(), row_size* num_rows);

    for (auto i = 0; i < buffer.size(); ++i) {
        EXPECT_EQ(int, buffer[i], px_value++);
    }
}

int
main()
{
    int retval = 1;

    ZarrStream* stream;
    ZarrStreamSettings settings;
    memset(&settings, 0, sizeof(settings));

    settings.version = ZarrVersion_2;
    settings.store_path = static_cast<const char*>(TEST ".zarr");
    settings.max_threads = 0;
    settings.data_type = ZarrDataType_uint8;

    try {
        // allocate dimensions
        configure_stream_dimensions(&settings);
        stream = ZarrStream_create(&settings);

        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings.store_path));

        // append partial frames
        std::vector<uint8_t> data(16, 0);
        for (auto row = 0; row < 96; ++row) { // 2 frames worth of data
            std::fill(data.begin(), data.end(), row);
            for (auto col_group = 0; col_group < 4; ++col_group) {
                const auto bytes_written =
                  stream->append(data.data(), data.size());
                EXPECT_EQ(int, data.size(), bytes_written);
            }
        }

        data.resize(2 * 48 * 64);
        std::iota(data.begin(), data.end(), 96);

        // append more than one frame, then fill in the rest
        const auto bytes_to_write = 48 * 64 + 7;
        auto bytes_written = stream->append(data.data(), bytes_to_write);
        EXPECT_EQ(int, bytes_to_write, bytes_written);

        bytes_written = stream->append(data.data() + bytes_to_write,
                                       data.size() - bytes_to_write);
        EXPECT_EQ(int, data.size() - bytes_to_write, bytes_written);

        verify_file_data(settings);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    // cleanup
    ZarrStreamSettings_destroy_dimension_array(&settings);
    ZarrStream_destroy(stream);

    std::error_code ec;
    if (fs::is_directory(settings.store_path) &&
        !fs::remove_all(settings.store_path, ec)) {
        LOG_ERROR("Failed to remove store path: ", ec.message().c_str());
    }
    return retval;
}