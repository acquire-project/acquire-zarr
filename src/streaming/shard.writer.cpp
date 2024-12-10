#include "shard.writer.hh"
#include "macros.hh"
#include "vectorized.file.writer.hh"

#include <algorithm>
#include <filesystem>
#include <limits>

namespace fs = std::filesystem;

#ifdef max
#undef max
#endif

zarr::ShardWriter::ShardWriter(std::string_view file_path,
                               uint32_t chunks_before_flush,
                               uint32_t chunks_per_shard)
  : file_path_(file_path)
  , chunks_before_flush_{ chunks_before_flush }
  , chunks_per_shard_{ chunks_per_shard }
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
        flush_();
    }
}

bool
zarr::ShardWriter::flush_()
{
    std::vector<std::span<std::byte>> buffers;
    buffers.reserve(chunks_.size() + 1);
    for (const auto& chunk : chunks_) {
        buffers.emplace_back(*chunk);
    }
    buffers.emplace_back(index_table_);

    try {
        VectorizedFileWriter writer(file_path_);
        writer.write_vectors(buffers, file_offset_);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to write chunk: ", std::string(exc.what()));
        return false;
    }

    chunks_flushed_ += chunks_.size();
    chunks_.clear();
    chunks_.reserve(chunks_before_flush_);
    file_offset_ = cumulative_size_;

    cv_.notify_all();

    return true;
}

void
zarr::ShardWriter::set_offset_extent_(uint32_t shard_internal_index,
                                      uint64_t offset,
                                      uint64_t size)
{
    EXPECT(shard_internal_index < chunks_per_shard_,
           "Shard internal index ",
           shard_internal_index,
           " out of bounds");

    auto* index_table_u64 = reinterpret_cast<uint64_t*>(index_table_.data());
    const auto index = 2 * shard_internal_index;
    index_table_u64[index] = offset;
    index_table_u64[index + 1] = size;
}

bool
zarr::finalize_shard_writer(std::unique_ptr<ShardWriter>&& writer)
{
    if (writer == nullptr) {
        LOG_INFO("Writer is null. Nothing to finalize.");
        return true;
    }

    if (!writer->flush_()) {
        return false;
    }

    // resize file if necessary
    const auto file_size = fs::file_size(writer->file_path_);
    const auto expected_size = writer->cumulative_size_ +
                               writer->chunks_per_shard_ * 2 * sizeof(uint64_t);
    if (file_size > expected_size) {
        fs::resize_file(writer->file_path_, expected_size);
    }

    writer.reset();
    return true;
}