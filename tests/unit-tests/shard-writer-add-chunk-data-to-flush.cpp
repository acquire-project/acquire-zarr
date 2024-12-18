#include "shard.writer.hh"
#include "thread.pool.hh"
#include "unit.test.macros.hh"

#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

static constexpr size_t chunk_size = 967;
static constexpr uint32_t chunks_before_flush = 2;
static constexpr uint32_t chunks_per_shard = 8;

void
verify_file_data(const std::string& filename, size_t file_size)
{
    std::ifstream file(filename, std::ios::binary);
    std::vector<std::byte> read_buffer(file_size);

    file.read(reinterpret_cast<char*>(read_buffer.data()), file_size);
    CHECK(file.good() && file.gcount() == file_size);

    int values[] = { 0, 7, 3, 1, 5, 6, 2, 4 };

    size_t buf_offset = 0;
    for (auto i = 0; i < 8; ++i) {
        if (i % chunks_before_flush == 0) {
            buf_offset = zarr::align_to_system_boundary(buf_offset);
        }

        for (size_t j = buf_offset; j < buf_offset + chunk_size; ++j) {
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
        buf_offset += chunk_size;
    }

    // check the index table
    const auto index_table_size = chunks_per_shard * 2 * sizeof(uint64_t);
    CHECK(buf_offset == file_size - index_table_size);
    auto* index_table =
      reinterpret_cast<uint64_t*>(read_buffer.data() + buf_offset);

    size_t file_offset = 0;

    // chunk 0
    EXPECT(index_table[0] == file_offset,
           "Expected ",
           file_offset,
           ", got ",
           index_table[0]);
    EXPECT(index_table[1] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[1]);

    // chunk 7
    EXPECT(index_table[14] == file_offset + chunk_size,
           "Expected ",
           file_offset + chunk_size,
           ", got ",
           index_table[14]);
    EXPECT(index_table[15] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[15]);

    file_offset =
      zarr::align_to_system_boundary(chunks_before_flush * chunk_size);

    // chunk 3
    EXPECT(index_table[6] == file_offset,
           "Expected ",
           file_offset,
           " got ",
           index_table[6]);
    EXPECT(index_table[7] == chunk_size,
           "Expected ",
           chunk_size,
           ", got ",
           index_table[7]);

    // chunk 1
    EXPECT(index_table[2] == file_offset + chunk_size,
           "Expected ",
           file_offset + chunk_size,
           ", got ",
           index_table[2]);
    EXPECT(index_table[3] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[3]);

    file_offset +=
      zarr::align_to_system_boundary(chunks_before_flush * chunk_size);

    // chunk 5
    EXPECT(index_table[10] == file_offset,
           "Expected ",
           file_offset,
           ", got ",
           index_table[10]);
    EXPECT(index_table[11] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[11]);

    // chunk 6
    EXPECT(index_table[12] == file_offset + chunk_size,
           "Expected ",
           file_offset + chunk_size,
           ", got ",
           index_table[12]);
    EXPECT(index_table[13] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[13]);

    file_offset +=
      zarr::align_to_system_boundary(chunks_before_flush * chunk_size);

    // chunk 2
    EXPECT(index_table[4] == file_offset,
           "Expected ",
           file_offset,
           ", got ",
           index_table[4]);
    EXPECT(index_table[5] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[5]);

    // chunk 4
    EXPECT(index_table[8] == file_offset + chunk_size,
           "Expected ",
           file_offset + chunk_size,
           ", got ",
           index_table[8]);
    EXPECT(index_table[9] == chunk_size,
           "Expected ",
           chunk_size,
           " got ",
           index_table[9]);
}

int
main()
{
    int retval = 1;

    auto shard_file_path = fs::temp_directory_path() / "shard-data.bin";
    zarr::ShardWriterConfig config = { .file_path = shard_file_path.string(),
                                       .chunks_before_flush =
                                         chunks_before_flush,
                                       .chunks_per_shard = chunks_per_shard };
    auto writer = std::make_unique<zarr::ShardWriter>(config);

    const auto index_table_size = chunks_per_shard * 2 * sizeof(uint64_t);

    std::vector<std::vector<std::byte>> chunk_data(chunks_per_shard);
    for (auto i = 0; i < chunks_per_shard; ++i) {
        chunk_data[i].resize(chunk_size);
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

        auto expected_file_size =
          3 * zarr::align_to_system_boundary(
                chunks_before_flush * chunk_size) + // 3 aligned sets of chunks
          chunks_before_flush * chunk_size + // final, unaligned set of chunks
          index_table_size;                  // index table
        auto actual_file_size = fs::file_size(shard_file_path);
        EXPECT(actual_file_size == expected_file_size,
               "Expected a file size of ",
               expected_file_size,
               ", got ",
               actual_file_size);

        verify_file_data(shard_file_path.string(), actual_file_size);

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