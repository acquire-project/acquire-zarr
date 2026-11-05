#pragma once

#include "array.base.hh"
#include "chunk.hh"
#include "definitions.hh"
#include "file.sink.hh"
#include "s3.client.hh"
#include "shard.hh"
#include "thread.pool.hh"

namespace zarr {
/**
 * @brief The largest buffer that compressing @p uncompressed_size bytes with
 * @p params can need.
 */
size_t
max_compressed_size(size_t uncompressed_size,
                    const std::optional<CompressionParams>& params);

/**
 * @brief Bytes of one layer of chunk slots: one dim-1 band when the array
 * flushes band by band (@see czbiohub-sf/livescreen-acquisition#210).
 */
size_t
chunk_layer_bytes(const ArrayDimensions& dims);

/**
 * @brief Uncompressed chunk bytes that may wait in, or run as, chunk jobs.
 * @details Two layers (two bands when banding). A full-resolution array gets
 * at least four chunks per worker as well, so that small layers or bands do
 * not leave workers idle; one pool thread is the frame thread, so it is not a
 * worker. Downsampled levels keep two layers. The bound is still independent
 * of the array size along dim 1, which is what banding protects.
 * ZarrStreamSettings_estimate_max_memory_usage calls this for every level, so
 * the estimate and the stream use one rule.
 */
size_t
chunk_inflight_budget(const ArrayDimensions& dims,
                      uint32_t n_threads,
                      uint16_t level_of_detail);

class MultiscaleArray;

class Array : public ArrayBase
{
  public:
    Array(std::shared_ptr<ArrayConfig> config,
          std::shared_ptr<ThreadPool> thread_pool,
          std::shared_ptr<FileHandlePool> file_handle_pool,
          std::shared_ptr<S3Client> s3_client);

    // Waits for outstanding writes, which hold `this`, before members go away.
    ~Array() override;

    size_t memory_usage() const noexcept override;

    [[nodiscard]] WriteResult write_frame(std::vector<uint8_t>& frame,
                                          size_t& bytes_written,
                                          uint64_t frame_id) override;
    size_t max_bytes() const override;

  protected:
    std::vector<std::shared_ptr<Chunk>> chunks_;
    mutable std::vector<std::mutex> chunk_mutexes_;

    std::vector<std::shared_ptr<Shard>> shards_;
    mutable std::mutex shards_mutex_;

    std::atomic<size_t> write_counter_;
    std::mutex write_counter_mutex_;
    std::condition_variable write_counter_cv_;

    // Uncompressed bytes held by dispatched chunk jobs. Changed under
    // write_counter_mutex_; read without it by memory_usage().
    std::atomic<size_t> inflight_bytes_{ 0 };
    // chunk_inflight_budget()
    const size_t inflight_budget_;

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

    /**
     * @brief Count one outstanding write that holds @p nbytes of chunk data.
     * @details Blocks while the bytes in flight would exceed the budget, so a
     * compressor or a sink that is slower than the frame thread slows the
     * frame thread, then the frame queue, then append(). Jobs in the pool
     * always run (the pool drains its queue even after a fatal error), so
     * the wait ends. The budget is at least two chunks; the wait also ends
     * when nothing is in flight, so that no chunk can wait forever.
     */
    void begin_write_(size_t nbytes);

    // Retire one outstanding write, release its bytes, and wake the waiters.
    void finish_write_(size_t nbytes = 0);

    void rollover_();
    void close_sinks_();

    // Explicitly finalize all live shards, returning false if any flush failed.
    // Only safe to call once outstanding writes have drained.
    [[nodiscard]] bool finalize_shards_();

    size_t frames_written_() const;

    friend class MultiscaleArray;
};
} // namespace zarr
