#include "macros.hh"
#include "shard.writer.hh"
#include "zarr.common.hh"

#include <algorithm>
#include <filesystem>
#include <limits>

namespace fs = std::filesystem;

#ifdef max
#undef max
#endif

zarr::ShardWriter::ShardWriter(const ShardWriterConfig& config)
  : file_path_(config.file_path)
  , file_{ std::make_unique<VectorizedFile>(config.file_path) }
  , chunks_before_flush_{ config.chunks_before_flush }
  , chunks_per_shard_{ config.chunks_per_shard }
  , chunks_flushed_{ 0 }
  , cumulative_size_{ 0 }
  , file_offset_{ 0 }
  , index_table_(2 * config.chunks_per_shard * sizeof(uint64_t))
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

    // chunks have been flushed and the new offset is aligned to the sector size
    if (chunks_.empty()) {
        cumulative_size_ = file_offset_;
    }

    set_offset_extent_(index_in_shard, cumulative_size_, buffer->size());
    cumulative_size_ += buffer->size();

    chunks_.push_back(buffer);
    if (chunks_.size() == chunks_before_flush_) {
        CHECK(flush_());
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
        file_write_vectorized(*file_, buffers, file_offset_);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to write chunk: ", std::string(exc.what()));
        return false;
    }

    chunks_flushed_ += chunks_.size();
    chunks_.clear();
    chunks_.reserve(chunks_before_flush_);
    file_offset_ = zarr::align_to_system_boundary(cumulative_size_);

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

    // flush remaining chunks and close file
    if (!writer->flush_()) {
        LOG_ERROR("Failed to flush remaining chunks.");
        return false;
    }
    writer->file_.reset(); // release file handle

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

bool
zarr::make_shard_writers(
  std::string_view base_path,
  uint32_t chunks_before_flush,
  uint32_t chunks_per_shard,
  const ArrayDimensions& dimensions,
  std::shared_ptr<ThreadPool> thread_pool,
  std::vector<std::unique_ptr<ShardWriter>>& shard_writers)
{
    auto paths =
      construct_data_paths(base_path, dimensions, shards_along_dimension);

    auto parent_paths = get_parent_paths(paths);
    if (!make_dirs(parent_paths, thread_pool)) {
        LOG_ERROR("Failed to create dataset paths.");
        return false;
    }

    shard_writers.clear();
    shard_writers.reserve(paths.size());

    for (const auto& path : paths) {
        ShardWriterConfig config{ .file_path = path,
                                  .chunks_before_flush = chunks_before_flush,
                                  .chunks_per_shard = chunks_per_shard };
        shard_writers.emplace_back(std::make_unique<ShardWriter>(config));
    }

    return true;
}
