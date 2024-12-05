#include "shard.writer.hh"
#include "macros.hh"
#include "vectorized.file.writer.hh"

#include <algorithm>
#include <limits>

#ifdef max
#undef max
#endif

zarr::ShardWriter::ShardWriter(std::shared_ptr<ThreadPool> thread_pool,
                               uint32_t chunks_before_flush,
                               uint32_t chunks_per_shard)
  : thread_pool_(thread_pool)
  , chunks_before_flush_{ chunks_before_flush }
  , chunks_flushed_{ 0 }
  , cumulative_size_{ 0 }
  , file_offset_{ 0 }
  , index_table_(2 * chunks_per_shard * sizeof(uint64_t))
{
    std::fill_n(reinterpret_cast<uint64_t*>(index_table_.data()),
                index_table_.size() / sizeof(uint64_t),
                std::numeric_limits<uint64_t>::max());

    chunks_.reserve(chunks_before_flush_);
}

void
zarr::ShardWriter::add_chunk(ChunkBufferPtr buffer, uint32_t index_in_shard)
{
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] { return chunks_.size() < chunks_before_flush_; });

    set_offset_extent_(index_in_shard, cumulative_size_, buffer->size());
    cumulative_size_ += buffer->size();

    chunks_.push_back(buffer);
    if (chunks_.size() == chunks_before_flush_) {
        auto job = [this](std::string& err) -> bool {
            std::vector<std::span<std::byte>> buffers;
            buffers.reserve(chunks_.size() + 1);
            for (const auto& chunk : chunks_) {
                buffers.push_back(std::span(*chunk));
            }
            buffers.push_back(std::span(index_table_));
            //            VectorizedFileWriter writer()


            chunks_.clear();
            cv_.notify_all();
            return true;
        };

        EXPECT(thread_pool_->push_job(job),
               "Failed to push job to thread pool.");
    }
}

void
zarr::ShardWriter::set_offset_extent_(uint32_t shard_internal_index,
                                      uint64_t offset,
                                      uint64_t size)
{
    auto* index_table_u64 = reinterpret_cast<uint64_t*>(index_table_.data());
    const auto index = 2 * shard_internal_index;
    index_table_u64[index] = offset;
    index_table_u64[index + 1] = size;
}