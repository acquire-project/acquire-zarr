#include "shard.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

constexpr size_t chunks_per_shard = 2;
constexpr size_t bytes_per_chunk = 32;

size_t
expected_shard_size(size_t n_written_chunks)
{
    // written chunk data + index table (offset+extent per chunk) + crc32
    return n_written_chunks * bytes_per_chunk +
           2 * chunks_per_shard * sizeof(uint64_t) + sizeof(uint32_t);
}

zarr::ShardConfig
make_config(const std::string& path)
{
    return zarr::ShardConfig{
        .path = path,
        .chunks_per_shard = chunks_per_shard,
        .bytes_per_chunk = bytes_per_chunk,
        .bucket_name = std::nullopt,
    };
}
} // namespace

// A complete shard finalizes itself from its last chunk writer, and that
// finalization is reported through write_chunk's return value.
void
test_complete_shard_finalizes_from_last_writer()
{
    const auto path = (base_dir / "complete").string();
    const std::vector<uint8_t> buffer(bytes_per_chunk, 0xAB);

    auto pool = std::make_shared<zarr::FileHandlePool>();
    {
        zarr::Shard shard(make_config(path), pool, nullptr);

        CHECK(shard.write_chunk(0, buffer)); // not the last writer
        CHECK(shard.write_chunk(1, buffer)); // last writer: finalizes the shard

        // explicit finalize after completion is idempotent and still succeeds
        CHECK(shard.finalize());
        CHECK(shard.finalize());
    }

    CHECK(fs::is_regular_file(path));
    EXPECT_EQ(size_t, fs::file_size(path), expected_shard_size(2));
}

// An incomplete shard (e.g. a partial trailing shard at close) is finalized
// explicitly and reports success, writing the table for the chunks it has.
void
test_partial_shard_finalizes_explicitly()
{
    const auto path = (base_dir / "partial").string();
    const std::vector<uint8_t> buffer(bytes_per_chunk, 0xCD);

    auto pool = std::make_shared<zarr::FileHandlePool>();
    {
        zarr::Shard shard(make_config(path), pool, nullptr);

        CHECK(shard.write_chunk(0, buffer)); // only one of two chunks

        // last writer never fired; explicit finalize must flush the table
        CHECK(shard.finalize());
    }

    CHECK(fs::is_regular_file(path));
    EXPECT_EQ(size_t, fs::file_size(path), expected_shard_size(1));
}

int
main()
{
    int retval = 0;

    try {
        fs::remove_all(base_dir);
        fs::create_directories(base_dir);

        test_complete_shard_finalizes_from_last_writer();
        test_partial_shard_finalizes_explicitly();
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
        retval = 1;
    }

    std::error_code ec;
    fs::remove_all(base_dir, ec);

    return retval;
}
