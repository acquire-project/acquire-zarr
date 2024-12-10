#include "shard.writer.hh"
#include "thread.pool.hh"
#include "unit.test.macros.hh"

#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

void
verify_file_data(const std::string& filename)
{
    const size_t file_size = 1024 * 8;
    std::ifstream file(filename, std::ios::binary);
    std::vector<std::byte> read_buffer(file_size);

    file.read(reinterpret_cast<char*>(read_buffer.data()), file_size);
    CHECK(file.good() && file.gcount() == file_size);

    int values[] = { 0, 7, 3, 1, 5, 6, 2, 4 };

    size_t offset = 0;
    for (size_t i = 0; i < 8; ++i) {
        for (size_t j = offset; j < offset + 1024; ++j) {
            auto byte = (int)read_buffer[j];
            EXPECT(byte == values[i],
                   "Data mismatch at offset ",
                   j,
                   ". Expected ",
                   i,
                   " got ",
                   byte,
                   ".");
        }
        offset += 1024;
    }

    // check the index table
    std::vector<uint64_t> index_table(16);
    file.read(reinterpret_cast<char*>(index_table.data()),
              2 * 8 * sizeof(uint64_t));
    CHECK(file.good() && file.gcount() == 2 * 8 * sizeof(uint64_t));

    // chunk 0
    EXPECT(index_table[0] == 0, "Expected 0, got ", index_table[0]);
    EXPECT(index_table[1] == 1024, "Expected 1024, got ", index_table[1]);

    // chunk 1
    EXPECT(index_table[2] == 3 * 1024, "Expected 3072, got ", index_table[2]);
    EXPECT(index_table[3] == 1024, "Expected 1024, got ", index_table[3]);

    // chunk 2
    EXPECT(index_table[4] == 6 * 1024, "Expected 6144, got ", index_table[4]);
    EXPECT(index_table[5] == 1024, "Expected 1024, got ", index_table[5]);

    // chunk 3
    EXPECT(index_table[6] == 2 * 1024, "Expected 2048, got ", index_table[6]);
    EXPECT(index_table[7] == 1024, "Expected 1024, got ", index_table[7]);

    // chunk 4
    EXPECT(index_table[8] == 7 * 1024, "Expected 7168, got ", index_table[8]);
    EXPECT(index_table[9] == 1024, "Expected 1024, got ", index_table[9]);

    // chunk 5
    EXPECT(index_table[10] == 4 * 1024, "Expected 4096, got ", index_table[10]);
    EXPECT(index_table[11] == 1024, "Expected 1024, got ", index_table[11]);

    // chunk 6
    EXPECT(index_table[12] == 5 * 1024, "Expected 5120, got ", index_table[12]);
    EXPECT(index_table[13] == 1024, "Expected 1024, got ", index_table[13]);

    // chunk 7
    EXPECT(index_table[14] == 1 * 1024, "Expected 1024, got ", index_table[14]);
    EXPECT(index_table[15] == 1024, "Expected 1024, got ", index_table[15]);
}

int
main()
{
    int retval = 1;

    const uint32_t chunks_before_flush = 2;
    const uint32_t chunks_per_shard = 8;

    auto shard_file_path = fs::temp_directory_path() / "shard-data.bin";
    auto writer = std::make_unique<zarr::ShardWriter>(
      shard_file_path.string(), chunks_before_flush, chunks_per_shard);

    const auto index_table_size = chunks_per_shard * 2 * sizeof(uint64_t);

    std::vector<std::vector<std::byte>> chunk_data(chunks_per_shard);
    for (auto i = 0; i < chunks_per_shard; ++i) {
        chunk_data[i].resize(1024);
        std::fill(chunk_data[i].begin(), chunk_data[i].end(), std::byte(i));
    }

    try {
        writer->add_chunk(&chunk_data[0], 0);
        writer->add_chunk(&chunk_data[7], 7);
        writer->add_chunk(&chunk_data[3], 3);
        writer->add_chunk(&chunk_data[1], 1);
        writer->add_chunk(&chunk_data[5], 5);
        writer->add_chunk(&chunk_data[6], 6);
        writer->add_chunk(&chunk_data[2], 2);
        writer->add_chunk(&chunk_data[4], 4);
        zarr::finalize_shard_writer(std::move(writer));

        auto expected_file_size = 1024 * 8 + index_table_size;
        auto actual_file_size = fs::file_size(shard_file_path);
        EXPECT(actual_file_size == expected_file_size,
               "Expected a file size of ",
               expected_file_size,
               ", got ",
               actual_file_size);

        verify_file_data(shard_file_path.string());

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    // cleanup
    if (fs::exists(shard_file_path)) {
        fs::remove(shard_file_path);
    }

    return retval;
}