#pragma once

#include "buffer.pool.hh"
#include "compression.params.hh"
#include "definitions.hh"

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace zarr {
class Chunk
{
  public:
    // Takes a zeroed buffer from `pool` and returns it there on destruction.
    Chunk(std::shared_ptr<BufferPool> pool, size_t bytes_per_px);
    ~Chunk();

    // Copy a frame's tile directly into the chunk buffer, row by row, without
    // an intermediate per-tile heap buffer. Row r of `copy_nbytes` bytes is
    // read from `src + r*src_row_stride` and written at
    // `internal_offset + r*dst_row_stride`. Bytes in each destination row past
    // copy_nbytes (edge-tile padding) are left untouched (zero-initialized).
    // Sets has_data if any copied byte is nonzero.
    void write_tile_rows(uint64_t internal_offset,
                         const uint8_t* src,
                         size_t src_row_stride,
                         size_t copy_nbytes,
                         size_t dst_row_stride,
                         uint32_t n_rows);

    const std::vector<uint8_t>& buffer();

    bool has_data() const;
    size_t size_bytes() const;

    /**
     * @brief Encode the chunk for its shard.
     * @param compression_params Compression to apply, if any.
     * @param output Scratch for the compressed bytes, at least
     * compressed_size_bound() of the chunk's size. Unused without compression.
     * @param[out] encoded The bytes to store: the front of @p output when
     * compressing, otherwise the chunk's own buffer, valid while the chunk
     * lives.
     * @return True if and only if encoding succeeded.
     */
    bool encode(const std::optional<CompressionParams>& compression_params,
                ByteSpan output,
                ConstByteSpan& encoded);

  private:
    const std::shared_ptr<BufferPool> pool_;
    const size_t size_bytes_;
    const size_t bytes_per_px_;

    std::mutex mutex_;
    std::vector<uint8_t> buffer_;

    std::atomic<bool> has_data_;
};
} // namespace zarr