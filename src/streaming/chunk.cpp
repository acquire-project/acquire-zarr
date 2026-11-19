#include "chunk.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <algorithm>
#include <cstring>

zarr::Chunk::Chunk(std::shared_ptr<BufferPool> pool, size_t bytes_per_px)
  : pool_(std::move(pool))
  , size_bytes_(pool_ ? pool_->buffer_size() : 0)
  , bytes_per_px_(bytes_per_px)
  , has_data_(false)
{
    EXPECT(size_bytes_ > 0, "Empty chunk");

    // Every byte a frame does not write (edge-tile padding, the tail of a short
    // final chunk, a ragged internal dimension) must read as zero, so a reused
    // buffer is zeroed like a fresh one.
    buffer_ = pool_->acquire(true);
}

zarr::Chunk::~Chunk()
{
    pool_->release(std::move(buffer_));
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
zarr::Chunk::encode(const std::optional<CompressionParams>& compression_params,
                    ByteSpan output,
                    ConstByteSpan& encoded)
{
    std::unique_lock lock(mutex_);
    if (buffer_.empty()) {
        return false;
    }

    if (!compression_params) {
        encoded = ConstByteSpan(buffer_);
        return true;
    }

    const size_t n_bytes = std::visit(
      [this, output]<typename ParamT>(const ParamT& params) -> size_t {
          if constexpr (std::is_same_v<ParamT, BloscCompressionParams>) {
              return compress_into(buffer_, output, params, bytes_per_px_);
          } else {
              return compress_into(buffer_, output, params);
          }
      },
      *compression_params);

    if (n_bytes == 0) {
        return false;
    }

    // The compressed bytes are all the write needs, so the raw buffer goes back
    // to the pool now rather than staying resident through a slow sink write.
    pool_->release(std::move(buffer_));
    buffer_ = {};

    encoded = ConstByteSpan(output.data(), n_bytes);
    return true;
}
