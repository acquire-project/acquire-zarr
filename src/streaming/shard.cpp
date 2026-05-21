#include "macros.hh"
#include "shard.hh"

#include <crc32c/crc32c.h>

#include <cstring>

zarr::Shard::Shard(const ShardConfig& config,
                   std::shared_ptr<FileHandlePool> file_handle_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : offsets_(config.chunks_per_shard, std::numeric_limits<uint64_t>::max())
  , extents_(config.chunks_per_shard, std::numeric_limits<uint64_t>::max())
  , table_flushed_(false)
  , unwritten_chunks_(config.chunks_per_shard)
  , file_offset_(0)
  , file_handle_pool_(std::move(file_handle_pool))
  , s3_connection_pool_(std::move(s3_connection_pool))
  , path_(config.path)
  , bucket_name_(config.bucket_name)
{
    if (bucket_name_) {
        EXPECT(s3_connection_pool_, "S3 connection pool not given");
    } else {
        EXPECT(file_handle_pool_, "File handle pool not given");
    }

    make_sink_();
}

zarr::Shard::~Shard()
{
    try {
        std::unique_lock lock(mutex_);
        if (const bool res = table_flushed_ ? true : write_table_(); !res) {
            LOG_ERROR("Failed to write shard table.");
        }

        finalize_sink(std::move(sink_));
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize shard: ", exc.what());
    }
}

bool
zarr::Shard::write_chunk(uint32_t internal_index,
                         const std::vector<uint8_t>& buffer)
{
    EXPECT(internal_index < offsets_.size(),
           "Internal index ",
           internal_index,
           " out of bounds");

    extents_[internal_index] = buffer.size();

    std::unique_lock lock(mutex_);
    const uint64_t offset = offsets_[internal_index] = file_offset_;

    // if this write fails, the file offset will still be incremented by the
    // size of this chunk, so a subsequent retry writes to a new offset
    file_offset_ += buffer.size();

    lock.unlock();
    bool res = sink_->write(offset, buffer);
    if (!res) {
        LOG_ERROR("Failed to write chunk");
        return false;
    }
    lock.lock();

    CHECK(unwritten_chunks_ > 0);

    // fetch_sub returns the value immediately preceding mutation
    if (unwritten_chunks_.fetch_sub(1) == 1) {
        res = write_table_();
    }
    cv_.notify_all();

    return res;
}

bool
zarr::Shard::skip_chunk(uint32_t internal_index)
{
    bool res = true;

    offsets_[internal_index] = std::numeric_limits<uint64_t>::max();
    extents_[internal_index] = std::numeric_limits<uint64_t>::max();

    std::unique_lock lock(mutex_);
    CHECK(unwritten_chunks_ > 0);

    // fetch_sub returns the value immediately preceding mutation
    if (unwritten_chunks_.fetch_sub(1) == 1) {
        res = write_table_();
    }
    cv_.notify_all();

    return res;
}

void
zarr::Shard::make_sink_()
{
    if (s3_connection_pool_) {
        sink_ = make_s3_sink(*bucket_name_, path_, s3_connection_pool_);
    } else {
        sink_ = make_file_sink(path_, file_handle_pool_);
    }
}

bool
zarr::Shard::write_table_()
{
    const size_t n_chunks = offsets_.size();
    const size_t table_size_bytes = 2 * n_chunks * sizeof(uint64_t);
    std::vector<uint8_t> table(table_size_bytes + sizeof(uint32_t));
    auto* table_ptr = table.data();

    auto* table_u64 = reinterpret_cast<uint64_t*>(table_ptr);

    for (auto i = 0; i < n_chunks; ++i) {
        table_u64[2 * i] = offsets_[i];
        table_u64[2 * i + 1] = extents_[i];
    }

    // compute crc32 checksum of the table
    auto* checksum = reinterpret_cast<uint32_t*>(table_ptr + table_size_bytes);
    *checksum = crc32c::Crc32c(table_ptr, table_size_bytes);

    return table_flushed_ = sink_->write(file_offset_, table);
}
