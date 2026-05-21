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
  , is_compressed_(false)
{
    EXPECT(size_bytes_ > 0, "Empty chunk");
}

void
zarr::Chunk::write_tile(uint64_t internal_offset, std::vector<uint8_t>&& tile)
{
    EXPECT(internal_offset + tile.size() <= buffer_.size(),
           "Cannot write ",
           tile.size(),
           " bytes at offset ",
           internal_offset,
           " to buffer of size ",
           buffer_.size(),
           ".");

    const bool any_nonzero =
      std::ranges::any_of(tile, [](uint8_t b) { return b != 0; });
    if (!any_nonzero) {
        return;
    }

    std::unique_lock lock(mutex_);
    has_data_ = true;
    memcpy(buffer_.data() + internal_offset, tile.data(), tile.size());
}

const std::vector<uint8_t>&
zarr::Chunk::buffer()
{
    return buffer_;
}

bool
zarr::Chunk::has_data() const
{
    return has_data_;
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
    if (!compression_params) {
        std::unique_lock lock(mutex_);
        data.resize(buffer_.size());
        std::ranges::copy(buffer_.begin(), buffer_.end(), data.begin());

        return true;
    }

    std::unique_lock lock(mutex_);
    if (is_compressed_) {
        data.resize(buffer_.size());
        std::ranges::copy(buffer_.begin(), buffer_.end(), data.begin());

        return true;
    }

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

    data.resize(buffer_.size());
    std::ranges::copy(buffer_.begin(), buffer_.end(), data.begin());
    return is_compressed_ = true;
}