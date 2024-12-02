#pragma once

#include "array.writer.hh"

namespace zarr {
class ChunkIndexBuffer
{
  public:
    ChunkIndexBuffer() { ready_chunks_.reserve(MAX_BUFFER_SIZE_); }

    bool try_add_chunk(uint32_t chunk_buffer_index)
    {
        std::lock_guard lock(mutex_);
        if (ready_chunks_.size() >= MAX_BUFFER_SIZE_) {
            return false;
        }

        ready_chunks_.push_back(chunk_buffer_index);
        ++ready_count_;
        return true;
    }

    std::vector<uint32_t> take_chunks()
    {
        std::lock_guard lock(mutex_);
        std::vector<uint32_t> chunks = std::move(ready_chunks_);
        ready_chunks_.clear();
        ready_count_ = 0;
        return chunks;
    }

  private:
    static constexpr size_t MAX_BUFFER_SIZE_ = 16;

    std::mutex mutex_;
    std::vector<uint32_t> ready_chunks_;
    uint32_t ready_count_{ 0 };
};
class ZarrV2ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV2ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool);

    ZarrV2ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    ChunkIndexBuffer ready_chunks_;

    std::string data_root_() const override;
    std::string metadata_path_() const override;
    PartsAlongDimensionFun parts_along_dimension_() const override;
    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
