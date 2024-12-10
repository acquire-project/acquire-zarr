#pragma once

#include "thread.pool.hh"

#include <condition_variable>
#include <latch>
#include <mutex>
#include <vector>
#include <atomic>

namespace zarr {
class ShardWriter
{
  public:
    using ChunkBufferPtr = std::vector<std::byte>*;

    ShardWriter(std::string_view file_path,
                uint32_t chunks_before_flush,
                uint32_t chunks_per_shard);
    ~ShardWriter() = default;

    void add_chunk(ChunkBufferPtr buffer, uint32_t index_in_shard);

  private:
    uint32_t chunks_before_flush_;
    uint32_t chunks_per_shard_;
    uint32_t chunks_flushed_;
    std::string file_path_;

    std::vector<std::byte> index_table_;

    std::mutex mutex_;
    std::condition_variable cv_;

    std::vector<ChunkBufferPtr> chunks_;
    uint64_t cumulative_size_;
    uint64_t file_offset_;

    void set_offset_extent_(uint32_t shard_internal_index,
                            uint64_t offset,
                            uint64_t size);
    [[nodiscard]] bool flush_();

    friend bool finalize_shard_writer(std::unique_ptr<ShardWriter>&& writer);
};

bool
finalize_shard_writer(std::unique_ptr<ShardWriter>&& writer);
} // namespace zarr