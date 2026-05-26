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