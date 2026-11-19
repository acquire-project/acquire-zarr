#include "chunk.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <vector>

/**
 * Compressing a chunk returns its raw buffer to the pool at once, so a chunk
 * being written holds only its compressed copy, not both. Without compression
 * the chunk's own buffer is what gets written, so it is kept until the chunk
 * is destroyed.
 */
namespace {
constexpr size_t rows = 32, row_bytes = 64, chunk_bytes = rows * row_bytes;

void
fill(zarr::Chunk& chunk)
{
    std::vector<uint8_t> src(chunk_bytes);
    for (size_t i = 0; i < src.size(); ++i) {
        src[i] = static_cast<uint8_t>(1 + i % 7);
    }
    chunk.write_tile_rows(0, src.data(), row_bytes, row_bytes, row_bytes, rows);
}

void
test_compressing_returns_the_buffer()
{
    auto pool = std::make_shared<zarr::BufferPool>(chunk_bytes, 2);
    const zarr::CompressionParams params =
      zarr::BloscCompressionParams("lz4", 1, 1);
    ByteVector output(zarr::compressed_size_bound(chunk_bytes, params));

    zarr::Chunk chunk(pool, 2);
    fill(chunk);
    EXPECT_EQ(size_t, pool->retained_bytes(), 0);

    ConstByteSpan encoded;
    CHECK(chunk.encode(params, output, encoded));
    CHECK(!encoded.empty());
    CHECK(encoded.data() == output.data());

    // the raw buffer is back in the pool while the compressed copy is written
    EXPECT_EQ(size_t, pool->retained_bytes(), chunk_bytes);

    // and the chunk cannot be encoded again
    CHECK(!chunk.encode(params, output, encoded));
}

void
test_uncompressed_keeps_the_buffer()
{
    auto pool = std::make_shared<zarr::BufferPool>(chunk_bytes, 2);

    {
        zarr::Chunk chunk(pool, 2);
        fill(chunk);

        ConstByteSpan encoded;
        CHECK(chunk.encode(std::nullopt, {}, encoded));
        EXPECT_EQ(size_t, encoded.size(), chunk_bytes);

        // the chunk's own buffer is what gets written
        EXPECT_EQ(size_t, pool->retained_bytes(), 0);
    }

    // destroying the chunk returns it
    EXPECT_EQ(size_t, pool->retained_bytes(), chunk_bytes);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        test_compressing_returns_the_buffer();
        test_uncompressed_keeps_the_buffer();
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
