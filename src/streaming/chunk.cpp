#include "chunk.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <algorithm>
#include <cstring>

zarr::Chunk::Chunk(size_t size_bytes, size_t bytes_per_px)
  : size_bytes_(size_bytes)
  , bytes_per_px_(bytes_per_px)
  , buffer_(size_bytes_, 0)
  , has_data_(false)
{
    EXPECT(size_bytes_ > 0, "Empty chunk");
}

void
zarr::Chunk::write_tile_rows(uint64_t internal_offset,
                             const uint8_t* src,
                             size_t src_row_stride,
                             size_t copy_nbytes,
                             size_t dst_row_stride,
                             uint32_t n_rows)
{
    if (n_rows == 0 || copy_nbytes == 0) {
        return;
    }

    const uint64_t span =
      static_cast<uint64_t>(n_rows - 1) * dst_row_stride + copy_nbytes;
    EXPECT(internal_offset + span <= buffer_.size(),
           "Cannot write ",
           span,
           " bytes at offset ",
           internal_offset,
           " to buffer of size ",
           buffer_.size(),
           ".");

    std::unique_lock lock(mutex_);
    uint8_t* dst = buffer_.data() + internal_offset;
    bool any_nonzero = has_data_.load(std::memory_order_relaxed);

    for (uint32_t r = 0; r < n_rows; ++r) {
        const uint8_t* s = src + static_cast<size_t>(r) * src_row_stride;
        memcpy(dst + static_cast<size_t>(r) * dst_row_stride, s, copy_nbytes);
        // Detect the chunk's first nonzero data to preserve the all-zero-chunk
        // skip optimization; stop scanning once we know it has data.
        if (!any_nonzero) {
            any_nonzero = std::any_of(
              s, s + copy_nbytes, [](uint8_t b) { return b != 0; });
        }
    }

    if (any_nonzero) {
        has_data_.store(true, std::memory_order_relaxed);
    }
}

const std::vector<uint8_t>&
zarr::Chunk::buffer()
{
    return buffer_;
}

bool
zarr::Chunk::has_data() const
{
    return has_data_.load(std::memory_order_relaxed);
}

size_t
zarr::Chunk::size_bytes() const
{
    return size_bytes_;
}

bool
zarr::Chunk::compress_and_take_buffer(
  const std::optional<CompressionParams>& compression_params,
  std::vector<uint8_t>& data)
{
    std::unique_lock lock(mutex_);
    if (buffer_.empty()) {
        return false;
    }

    if (compression_params) {
        if (!std::visit(
              [this]<typename ParamT>(const ParamT& params) {
                  using T = std::decay_t<ParamT>;
                  if constexpr (std::is_same_v<T, BloscCompressionParams>) {
                      return compress_in_place(buffer_, params, bytes_per_px_);
                  } else {
                      return compress_in_place(buffer_, params);
                  }
              },
              *compression_params)) {
            return false;
        }
    }

    // single-shot: move the buffer out, leaving the chunk empty
    data = std::move(buffer_);
    return true;
}