#pragma once

#include "array.base.hh"
#include "definitions.hh"
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
    struct ShardLayer
    {
        size_t offset; // offset in bytes from start of shard
        std::vector<std::vector<uint8_t>> chunks;
    };

    std::vector<std::vector<uint8_t>> chunk_buffers_;
    std::vector<std::string> data_paths_;

    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    std::string data_root_;
    bool is_closing_;

    uint32_t current_layer_;
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;

    bool make_metadata_(std::string& metadata) override;
    [[nodiscard]] bool close_() override;

    void make_data_paths_();
    void fill_buffers_();

    bool should_flush_() const;
    bool should_rollover_() const;

    size_t write_frame_to_chunks_(std::vector<uint8_t>& data);

    [[nodiscard]] ShardLayer collect_chunks_(uint32_t shard_index);
    [[nodiscard]] bool compress_and_flush_data_();
    [[nodiscard]] bool compress_chunks_();
    void update_table_entries_();
    [[nodiscard]] virtual bool flush_data_() = 0;
    [[nodiscard]] virtual bool flush_tables_() = 0;
    void rollover_();
    virtual void close_io_streams_() = 0;

    friend class MultiscaleArray;
};
} // namespace zarr
