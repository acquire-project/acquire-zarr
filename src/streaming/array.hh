#pragma once

#include "array.base.hh"
#include "chunk.hh"
#include "definitions.hh"
#include "file.sink.hh"
#include "s3.connection.hh"
#include "shard.hh"
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

    [[nodiscard]] WriteResult write_frame(std::vector<uint8_t>& frame,
                                          size_t& bytes_written,
                                          uint64_t frame_id) override;
    size_t max_bytes() const override;

  protected:
    std::vector<std::shared_ptr<Chunk>> chunks_;
    mutable std::vector<std::mutex> chunk_mutexes_;

    std::vector<std::shared_ptr<Shard>> shards_;
    std::mutex shards_mutex_;

    std::atomic<size_t> write_counter_;
    std::mutex write_counter_mutex_;
    std::condition_variable write_counter_cv_;

    std::vector<std::string> data_paths_;

    const uint64_t max_bytes_;       // max number of bytes that can be written
    const uint64_t bytes_per_frame_; // number of bytes per frame
    uint64_t total_bytes_written_;   // total bytes written to the array
    uint64_t bytes_to_flush_; // bytes written to the array since last flush
    uint32_t append_chunk_index_;
    std::string data_root_;
    bool is_closing_;

    uint64_t last_successful_frame_id_;
    uint32_t current_layer_;

    // dim-1 bands flushed so far in the current layer (banding only)
    uint32_t flushed_band_count_;

    bool make_metadata_(nlohmann::json& metadata) override;
    [[nodiscard]] bool close_() override;

    bool is_s3_array_() const;

    void make_shards_();
    [[nodiscard]] std::unique_ptr<Sink> make_data_sink_(
      std::string_view path) const;

    bool should_flush_() const;
    bool should_rollover_() const;

    size_t write_frame_to_chunks_(std::vector<uint8_t>& frame);

    [[nodiscard]] bool compress_and_flush_data_();

    // Incremental flush along dimension 1, one chunk band at a time, to bound
    // peak memory. Used when dimensions->supports_dim1_banding().
    // @see czbiohub-sf/livescreen-acquisition#210
    [[nodiscard]] bool flush_completed_bands_();
    [[nodiscard]] bool flush_layer_remainder_();
    [[nodiscard]] bool compress_and_flush_band_(uint32_t band_idx,
                                                uint32_t n_bands);

    // Compress and write (or skip-if-empty) one chunk, freeing its slot.
    void dispatch_chunk_job_(std::shared_ptr<Shard> shard,
                             uint32_t chunk_idx,
                             uint32_t internal_idx,
                             uint32_t shard_idx,
                             uint32_t chunk_offset,
                             size_t bytes_per_chunk,
                             size_t bytes_per_px);
    // Skip a ragged-padding slot to complete the shard's countdown.
    void dispatch_skip_job_(std::shared_ptr<Shard> shard,
                            uint32_t internal_idx,
                            uint32_t shard_idx);

    void rollover_();
    void close_sinks_();

    // Explicitly finalize all live shards, returning false if any flush failed.
    // Only safe to call once outstanding writes have drained.
    [[nodiscard]] bool finalize_shards_();

    size_t frames_written_() const;

    friend class MultiscaleArray;
};
} // namespace zarr
