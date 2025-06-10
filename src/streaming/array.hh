#pragma once

#include "blosc.compression.params.hh"
#include "definitions.hh"
#include "file.sink.hh"
#include "node.hh"
#include "s3.connection.hh"
#include "thread.pool.hh"

namespace zarr {
struct ArrayConfig : public ZarrNodeConfig
{
    ArrayConfig() = default;
    ArrayConfig(std::string_view store_root,
                std::string_view array_key,
                std::optional<std::string> bucket_name,
                std::optional<BloscCompressionParams> compression_params,
                std::shared_ptr<ArrayDimensions> dimensions,
                ZarrDataType dtype,
                int level_of_detail)
      : ZarrNodeConfig(store_root,
                       array_key,
                       bucket_name,
                       compression_params,
                       dimensions,
                       dtype)
      , level_of_detail(level_of_detail)
    {
    }

    int level_of_detail{ 0 };
};

class Array : public ZarrNode
{
  public:
    Array(std::shared_ptr<ArrayConfig> config,
          std::shared_ptr<ThreadPool> thread_pool,
          std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    [[nodiscard]] size_t write_frame(ConstByteSpan) override;

  protected:
    /// Buffering
    std::vector<ByteVector> data_buffers_;

    /// Filesystem
    std::vector<std::string> data_paths_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    bool is_closing_;

    [[nodiscard]] bool close_() override;

    std::shared_ptr<ArrayConfig> array_config_() const;

    /**
     * @brief Compute the number of bytes to allocate for a single chunk.
     * @note Allocate the usual chunk size, plus the maximum Blosc overhead if
     * we're compressing.
     * @return The number of bytes to allocate per chunk.
     */
    size_t bytes_to_allocate_per_chunk_() const;

    bool is_s3_array_() const;
    virtual std::string data_root_() const = 0;
    virtual const DimensionPartsFun parts_along_dimension_() const = 0;

    void make_data_paths_();
    virtual void make_buffers_() = 0;

    virtual BytePtr get_chunk_data_(uint32_t index) = 0;

    bool should_flush_() const;
    virtual bool should_rollover_() const = 0;

    size_t write_frame_to_chunks_(std::span<const std::byte> data);

    [[nodiscard]] virtual bool compress_and_flush_data_() = 0;
    void rollover_();

    virtual void close_sinks_() = 0;

  private:
    std::mutex buffers_mutex_;

    friend bool finalize_array(std::unique_ptr<Array>&& array);
};

bool
finalize_array(std::unique_ptr<Array>&& array);
} // namespace zarr
