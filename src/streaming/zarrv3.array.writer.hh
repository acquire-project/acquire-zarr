#pragma once

#include "array.writer.hh"

namespace zarr {
struct ChunkIndex
{
    uint32_t buffer_idx;
    uint64_t offset;
    uint64_t size;
};

class ShardIndexBuffer
{
  public:
    ShardIndexBuffer() { ready_chunks_.reserve(MAX_BUFFER_SIZE_); }

    bool try_add_chunk(uint32_t chunk_buffer_index, uint64_t chunk_size)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (ready_chunks_.size() >= MAX_BUFFER_SIZE_) {
            return false;
        }

        ready_chunks_.push_back(
          { chunk_buffer_index, cumulative_size_, chunk_size });
        cumulative_size_ += chunk_size;
        ++ready_count_;
        return true;
    }

    std::vector<ChunkIndex> take_chunks()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<ChunkIndex> chunks = std::move(ready_chunks_);
        ready_chunks_.clear();
        ready_count_ = 0;
        return chunks;
    }

  private:
    static constexpr size_t MAX_BUFFER_SIZE_ = 16;

    std::mutex mutex_;
    std::vector<ChunkIndex> ready_chunks_;
    uint64_t cumulative_size_{ 0 };
    uint32_t ready_count_{ 0 };
};

struct ZarrV3ArrayWriter : public ArrayWriter
{
  public:
    ZarrV3ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool);
    ZarrV3ArrayWriter(
      const ArrayWriterConfig& config,
      std::shared_ptr<ThreadPool> thread_pool,
      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<ShardIndexBuffer> shards_ready_;

    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    std::string data_root_() const override;
    std::string metadata_path_() const override;
    PartsAlongDimensionFun parts_along_dimension_() const override;
    void compress_and_flush_() override;
    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
