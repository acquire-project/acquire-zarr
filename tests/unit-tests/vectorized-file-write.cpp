#include "vectorized.file.writer.hh"
#include "unit.test.macros.hh"

#include <iostream>
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

size_t
write_to_file(const std::string& filename)
{
    size_t file_size = 0;
    zarr::VectorizedFileWriter writer(filename);

    std::vector<std::vector<std::byte>> data(10);
    std::vector<std::span<std::byte>> spans(10);

    for (auto i = 0; i < data.size(); ++i) {
        data[i].resize((i + 1) * 1024);
        std::fill(data[i].begin(), data[i].end(), std::byte(i));
        file_size += data[i].size();
        spans[i] = data[i];
    }
    CHECK(writer.write_vectors(spans, 0));

    // write more data
    for (auto i = 0; i < 10; ++i) {
        auto& vec = data[i];
        std::fill(vec.begin(), vec.end(), std::byte(i + 10));
        spans[i] = vec;
    }
    CHECK(writer.write_vectors(spans, file_size));

    return 2 * file_size;
}

void
verify_file_data(const std::string& filename, size_t file_size)
{
    std::ifstream file(filename, std::ios::binary);
    std::vector<std::byte> read_buffer(file_size);

    file.read(reinterpret_cast<char*>(read_buffer.data()), file_size);
    CHECK(file.good() && file.gcount() == file_size);

    // Verify data pattern
    size_t offset = 0;
    for (size_t i = 0; i < 10; ++i) {
        size_t size = (i + 1) * 1024;

        for (size_t j = offset; j < offset + size; ++j) {
            auto byte = (int)read_buffer[j];
            EXPECT(byte == i,
                   "Data mismatch at offset ",
                   j,
                   ". Expected ",
                   i,
                   " got ",
                   byte,
                   ".");
        }
        offset += size;
    }

    for (size_t i = 0; i < 10; ++i) {
        size_t size = (i + 1) * 1024;

        for (size_t j = offset; j < offset + size; ++j) {
            auto byte = (int)read_buffer[j];
            EXPECT(byte == i + 10,
                   "Data mismatch at offset ",
                   j,
                   ". Expected ",
                   i + 10,
                   " got ",
                   byte,
                   ".");
        }
        offset += size;
    }
}

int
main()
{
    const auto base_dir = fs::temp_directory_path() / "vectorized-file-writer";
    if (!fs::exists(base_dir) && !fs::create_directories(base_dir)) {
        std::cerr << "Failed to create directory: " << base_dir << std::endl;
        return 1;
    }

    int retval = 1;
    const auto filename = (base_dir / "test.bin").string();

    try {
        const auto file_size = write_to_file(filename);
        EXPECT(fs::exists(filename), "File not found: ", filename);

        auto file_size_on_disk = fs::file_size(filename);
        EXPECT(file_size_on_disk >= file_size, // sum(1:10) * 1024 * 2
               "Expected file size of at least ",
               file_size,
               " bytes, got ",
               file_size_on_disk);
        verify_file_data(filename, file_size);

        retval = 0;
    } catch (const std::exception& exc) {
        std::cerr << "Exception: " << exc.what() << std::endl;
    }

    // cleanup
    if (fs::exists(base_dir) && !fs::remove_all(base_dir)) {
        std::cerr << "Failed to remove directory: " << base_dir << std::endl;
    }

    return retval;
}