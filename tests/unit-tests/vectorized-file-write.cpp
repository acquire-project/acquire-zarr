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
    std::vector<size_t> offsets(10);
    size_t offset = 0;
    for (auto i = 0; i < data.size(); ++i) {
        data[i].resize((i + 1) * 1024);
        std::fill(data[i].begin(), data[i].end(), std::byte(i));

        offsets[i] = offset;
        offset += data[i].size();
    }

    file_size = offsets.back() + data.back().size();
    CHECK(writer.write_vectors(data, offsets));

    return file_size;
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
    for (size_t i = 0; i < 10; i++) {
        size_t size = (i + 1) * 1024;

        for (size_t j = offset; j < offset + size; j++) {
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