#pragma once

#include "array.base.hh"
#include "blosc.compression.params.hh"
#include "definitions.hh"
#include "file.sink.hh"
#include "locked.buffer.hh"
#include "s3.connection.hh"
#include "thread.pool.hh"

namespace zarr {
class MultiscaleArray;

class Array : public ArrayBase
{
  public:
    Array(std::shared_ptr<ArrayConfig> config,
          std::shared_ptr<ThreadPool> thread_pool,
          std::shared_ptr<FileHandlePool> file_handle_pool,
          std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    size_t memory_usage() const noexcept override;

    [[nodiscard]] size_t write_frame(LockedBuffer&) override;

  protected:
    /// Buffering
    std::vector<LockedBuffer> chunk_buffers_;

    /// Filesystem
    std::vector<std::string> data_paths_;
    std::unordered_map<std::string, std::unique_ptr<Sink>> data_sinks_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    std::string data_root_;
    bool is_closing_;

    /// Sharding
    uint32_t current_layer_;
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;
    [[nodiscard]] bool close_() override;
    [[nodiscard]] bool close_impl_();

    bool is_s3_array_() const;

    void make_data_paths_();
    [[nodiscard]] std::unique_ptr<Sink> make_data_sink_(std::string_view path);
    void fill_buffers_();

    bool should_flush_() const;
    bool should_rollover_() const;

    size_t write_frame_to_chunks_(LockedBuffer& data);

    [[nodiscard]] ByteVector consolidate_chunks_(uint32_t shard_index);
    [[nodiscard]] bool compress_and_flush_data_();
    void rollover_();
    void close_sinks_();

    friend class MultiscaleArray;
};
} // namespace zarr
