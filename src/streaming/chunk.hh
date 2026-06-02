#pragma once

#include "compression.params.hh"

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>

namespace zarr {
class Chunk
{
  public:
    Chunk(size_t size_bytes, size_t bytes_per_px);
    ~Chunk() = default;

    void write_tile(uint64_t internal_offset, std::vector<uint8_t>&& tile);

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
    bool compress_and_take_buffer(
      const std::optional<CompressionParams>& compression_params,
      std::vector<uint8_t>& data);

  private:
    const size_t size_bytes_;
    const size_t bytes_per_px_;

    std::mutex mutex_;
    std::vector<uint8_t> buffer_;

    std::atomic<bool> has_data_;
};
} // namespace zarr