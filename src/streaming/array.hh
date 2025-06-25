#pragma once

#include "array.base.hh"
#include "blosc.compression.params.hh"
#include "definitions.hh"
#include "file.sink.hh"
#include "locked.buffer.hh"
#include "s3.connection.hh"
#include "thread.pool.hh"

namespace zarr {
class Array : public ArrayBase
{
  public:
    Array(std::shared_ptr<ArrayConfig> config,
          std::shared_ptr<ThreadPool> thread_pool,
          std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    [[nodiscard]] size_t write_frame(LockedBuffer&) override;

  protected:
    /// Buffering
    std::vector<LockedBuffer> chunk_buffers_;

    /// Filesystem
    std::vector<std::string> data_paths_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    bool is_closing_;

    [[nodiscard]] bool close_() override;

    bool is_s3_array_() const;
    virtual std::string data_root_() const = 0;
    virtual const DimensionPartsFun parts_along_dimension_() const = 0;

    void make_data_paths_();
    void fill_buffers_();

    bool should_flush_() const;
    virtual bool should_rollover_() const = 0;

    size_t write_frame_to_chunks_(LockedBuffer& data);

    [[nodiscard]] virtual bool compress_and_flush_data_() = 0;
    void rollover_();

    virtual void close_sinks_() = 0;

  private:
    friend bool finalize_array(std::unique_ptr<Array>&& array);
};

bool
finalize_array(std::unique_ptr<Array>&& array);
} // namespace zarr
