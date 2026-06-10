#include "macros.hh"
#include "shard.hh"

#include <crc32c/crc32c.h>

#include <cstring>

namespace {
// sentinel for "no chunk written at this index yet"
constexpr uint64_t kUnwrittenSentinel = std::numeric_limits<uint64_t>::max();
} // namespace

zarr::Shard::Shard(const ShardConfig& config,
                   std::shared_ptr<FileHandlePool> file_handle_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : offsets_(config.chunks_per_shard, kUnwrittenSentinel)
  , extents_(config.chunks_per_shard, kUnwrittenSentinel)
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
    // Best-effort: complete shards finalize themselves from the last chunk
    // writer (where a flush error can be returned to the caller), and the
    // trailing partial shard is finalized explicitly at close. This only does
    // real work when a shard is destroyed without either having happened (e.g.
    // an aborted stream), and a flush error here cannot propagate.
    try {
        std::unique_lock lock(mutex_);
        if (!finalize_unlocked_()) {
            LOG_ERROR("Failed to finalize shard ", path_);
        }
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize shard ", path_, ": ", exc.what());
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

    uint64_t &offset = offsets_[internal_index],
             &extent = extents_[internal_index];

    // offset and extent have already been written, so this is a retry; double
    // check that the chunk is at least the same size
    if (offset < kUnwrittenSentinel) {
        EXPECT(extent == buffer.size(),
               "Retrying chunk write with different data");
    } else {
        // First attempt: claim the offset under the lock. The stores to
        // extents_[i] and offsets_[i] are published to write_table_() via
        // the seq_cst fetch_sub on unwritten_chunks_ below.
        extent = buffer.size();

        std::unique_lock lock(mutex_);
        offset = file_offset_;
        file_offset_ += buffer.size();
    }

    if (!sink_->write(offset, buffer)) {
        LOG_ERROR("Failed to write chunk");
        return false;
    }

    CHECK(unwritten_chunks_ > 0);

    // last writer in the shard publishes the table and durably flushes the
    // shard, so a flush failure surfaces as this job's result
    bool res = true;
    if (unwritten_chunks_.fetch_sub(1) == 1) {
        std::unique_lock lock(mutex_);
        res = finalize_unlocked_();
    }
    cv_.notify_all();

    return res;
}

bool
zarr::Shard::skip_chunk(uint32_t internal_index)
{
    offsets_[internal_index] = kUnwrittenSentinel;
    extents_[internal_index] = kUnwrittenSentinel;

    CHECK(unwritten_chunks_ > 0);

    bool res = true;
    if (unwritten_chunks_.fetch_sub(1) == 1) {
        std::unique_lock lock(mutex_);
        res = finalize_unlocked_();
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

bool
zarr::Shard::finalize()
{
    std::unique_lock lock(mutex_);
    return finalize_unlocked_();
}

bool
zarr::Shard::finalize_unlocked_()
{
    if (finalized_) {
        return finalize_ok_;
    }
    finalized_ = true;

    bool ok = true;
    if (!table_flushed_ && !write_table_()) {
        LOG_ERROR("Failed to write shard table for ", path_);
        ok = false;
    }

    // finalize_sink durably flushes (fsync / multipart completion) and releases
    // the sink; a flush failure here is the signal that the shard did not land.
    if (!finalize_sink(std::move(sink_))) {
        LOG_ERROR("Failed to flush shard ", path_);
        ok = false;
    }

    return finalize_ok_ = ok;
}
