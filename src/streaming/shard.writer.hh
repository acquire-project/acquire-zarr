#pragma once

#include "thread.pool.hh"

#include <condition_variable>
#include <mutex>
#include <vector>

#ifdef min
#undef min
#endif

namespace zarr {
struct ChunkIndex
{
    uint32_t buffer_idx;
    uint64_t offset;
    uint64_t size;
};

class ShardWriter
{
  public:
    using ChunkBufferPtr = std::vector<std::byte>*;

    ShardWriter(std::shared_ptr<ThreadPool> thread_pool,
                uint32_t chunks_before_flush,
                uint32_t chunks_per_shard);

    void add_chunk(ChunkBufferPtr buffer, uint32_t chunk_buffer_index);

  private:
    uint32_t chunks_before_flush_;
    uint32_t chunks_per_shard_;
    uint32_t chunks_flushed_;

    std::vector<std::byte> index_table_;

    std::shared_ptr<ThreadPool> thread_pool_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<ChunkBufferPtr> chunks_;
    std::vector<ChunkIndex> ready_chunks_;
    std::vector<ChunkIndex> chunk_table_;
    uint64_t cumulative_size_;
    uint64_t file_offset_;

    void set_offset_extent_(uint32_t shard_internal_index,
                            uint64_t offset,
                            uint64_t size);
};
} // namespace zarr