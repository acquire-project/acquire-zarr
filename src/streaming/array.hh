#pragma once

#include "array.base.hh"
#include "thread.pool.hh"

namespace zarr {
class MultiscaleArray;

class Array : public ArrayBase
{
  public:
    Array(std::shared_ptr<ArrayConfig> config,
          std::shared_ptr<ThreadPool> thread_pool);

    size_t memory_usage() const noexcept override;

    [[nodiscard]] size_t write_frame(std::vector<uint8_t>&) override;

  protected:
    std::vector<std::vector<uint8_t>> chunk_buffers_;
    std::vector<std::string> data_paths_;

    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_shard_index_;
    std::string data_root_;
    bool is_closing_;

    uint32_t current_layer_;
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    bool make_metadata_(std::string& metadata) override;
    [[nodiscard]] bool close_() override;

    /**
     * @brief Construct the data paths for all shards in the array with the
     * current append shard index.
     */
    void make_data_paths_();

    /**
     * @brief Fill the chunk buffers with empty data, resizing as needed.
     */
    void fill_buffers_();

    /**
     * @brief Determine if we should flush the current chunk buffers to storage.
     * @return True if we should flush, false otherwise.
     */
    bool should_flush_layer_() const;

    /**
     * @brief Determine if we should roll over to a new shard along the append
     * dimension.
     * @return True if we should roll over, false otherwise.
     */
    bool should_rollover_() const;

    /**
     * @brief Write the given frame data into the chunk buffers.
     * @param data The frame data.
     * @return The number of bytes written.
     */
    size_t write_frame_to_chunks_(std::vector<uint8_t>& data);

    /**
     * @brief Finalize all current shard files and close their associated I/O
     * streams. Update the data root to point to the next shard index.
     */
    void close_shards_();

    /**
     * @brief Return the location of the shard index for this array ("start" or
     * "end").
     * @return The index location.
     */
    virtual std::string index_location_() const = 0;

    /**
     * @brief Compress and flush all data currently in the chunk buffers to the
     * underlying storage.
     * @return True on success, false on failure.
     */
    [[nodiscard]] virtual bool compress_and_flush_data_() = 0;

    /**
     * @brief Ensure all tables are flushed and close all open IO streams
     * associated with this array.
     */
    virtual void finalize_append_shard_() = 0;

    friend class MultiscaleArray;
};
} // namespace zarr
