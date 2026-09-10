// shard.hh
#pragma once

#include "chunk.hh"
#include "compression.params.hh"
#include "sink.hh"
#include "thread.pool.hh"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace zarr {

class FileHandlePool;
class S3ConnectionPool;

struct ShardConfig
{
    std::string path;
    size_t chunks_per_shard;
    size_t bytes_per_chunk;
    std::optional<std::string> bucket_name; // nullopt => filesystem
};

class Shard
{
  public:
    Shard(const ShardConfig& config,
          std::shared_ptr<FileHandlePool> file_handle_pool,
          std::shared_ptr<S3ConnectionPool> s3_connection_pool);
    ~Shard();

    [[nodiscard]] bool write_chunk(uint32_t internal_index,
                                   const std::vector<uint8_t>& buffer);
    [[nodiscard]] bool skip_chunk(uint32_t internal_index);

    /**
     * @brief Write the shard table (if needed), durably flush the shard to
     * storage, and release the underlying sink.
     * @details Idempotent: subsequent calls return the cached result without
     * repeating I/O. Complete shards finalize themselves from the last chunk
     * writer; this exists so callers can finalize incomplete shards (e.g. a
     * partial trailing shard at close) and observe the result, rather than
     * relying on the destructor, where a flush error cannot propagate.
     * @return True if and only if the table write and flush both succeeded.
     */
    [[nodiscard]] bool finalize();

    const std::string& path() const { return path_; }

  private:
    std::vector<uint64_t> offsets_;
    std::vector<uint64_t> extents_;
    std::atomic<bool> table_flushed_;
    bool finalized_{ false };   // guarded by mutex_
    bool finalize_ok_{ false }; // guarded by mutex_; valid once finalized_

    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<uint64_t> unwritten_chunks_;

    uint64_t file_offset_;

    std::shared_ptr<FileHandlePool> file_handle_pool_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    std::string path_;
    std::optional<std::string> bucket_name_;
    std::unique_ptr<Sink> sink_;

    void make_sink_();
    [[nodiscard]] bool write_table_();

    // Caller must hold mutex_.
    [[nodiscard]] bool finalize_unlocked_();
};

} // namespace zarr
