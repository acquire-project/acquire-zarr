#pragma once

#include "array.dimensions.hh"
#include "compression.params.hh"
#include "file.handle.hh"
#include "s3.client.hh"
#include "sink.hh"
#include "thread.pool.hh"
#include "zarr.types.h"

#include <nlohmann/json.hpp>

#include <string>

namespace zarr {
struct ArrayConfig
{
    ArrayConfig() = default;
    ArrayConfig(std::string_view store_root,
                std::string_view group_key,
                std::optional<std::string> bucket_name,
                std::optional<CompressionParams> compression_params,
                std::shared_ptr<ArrayDimensions> dimensions,
                ZarrDataType dtype,
                std::optional<ZarrDownsamplingMethod> downsampling_method,
                uint16_t level_of_detail,
                bool is_ngff,
                uint32_t max_levels = 0)
      : store_root(store_root)
      , node_key(group_key)
      , bucket_name(bucket_name)
      , compression_params(compression_params)
      , dimensions(std::move(dimensions))
      , dtype(dtype)
      , is_ngff(is_ngff)
      , downsampling_method(downsampling_method)
      , level_of_detail(level_of_detail)
      , max_levels(max_levels)
    {
        if (downsampling_method.has_value() &&
            *downsampling_method >= ZarrDownsamplingMethodCount) {
            throw std::runtime_error(
              "Invalid downsampling method: " +
              std::to_string(static_cast<int>(*downsampling_method)));
        }
    }

    virtual ~ArrayConfig() = default;

    std::string store_root;
    std::string node_key;
    std::optional<std::string> bucket_name;
    std::optional<CompressionParams> compression_params;
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
    bool is_ngff{ false };
    std::optional<ZarrDownsamplingMethod> downsampling_method;
    uint16_t level_of_detail;
    uint32_t max_levels{ 0 };
};

enum class WriteResult
{
    Ok,
    PartialWrite,
    OutOfBounds,
    FrameSizeMismatch,
    FrameOutOfOrder,   // frame ID gap detected; predecessor not yet written
};

class ArrayBase
{
  public:
    ArrayBase(std::shared_ptr<ArrayConfig> config,
              std::shared_ptr<ThreadPool> thread_pool,
              std::shared_ptr<FileHandlePool> file_handle_pool,
              std::shared_ptr<S3Client> s3_client);
    virtual ~ArrayBase() = default;

    /**
     * @brief Write custom metadata to the array.
     * @param key The key for the custom metadata, relative to 'attributes' in
     * the array metadata.
     * @param metadata The JSON value to write as custom metadata.
     * @return True on success, false on failure.
     */
    bool write_custom_metadata(const std::string& key,
                               const nlohmann::json& metadata);

    /**
     * @brief Get the amount of memory currently used by this Array, in bytes.
     * @details Callable from any thread while frames are being written. Best
     * effort: a buffer whose lock is held by a writer is omitted rather than
     * waited for, so the result can under-report and must not be treated as
     * exact. Never blocks the write path.
     * @return Memory used by this object, in bytes.
     */
    virtual size_t memory_usage() const noexcept = 0;

    /**
     * @brief Write a buffer of data to the node.
     * @param frame The data to write. If an X-Y transpose is indicated in
     * configuration, this transposes the frame.
     * @param bytes_written Set to the number of bytes written on success, or 0
     * on failure. Implementations MUST set this before returning.
     * @param frame_id Index of the frame to write.
     * @return WriteResult::Ok on success, WriteResult::PartialWrite if @p data
     * does not constitute a complete chunk, or WriteResult::OutOfBounds if
     * writing @p data would exceed the declared array bounds. No data is
     * written in the OutOfBounds case.
     */
    [[nodiscard]] virtual WriteResult write_frame(std::vector<uint8_t>& frame,
                                                  size_t& bytes_written,
                                                  uint64_t frame_id) = 0;

    /**
     * @brief Query the maximum number of bytes we can append to this array.
     * @return The maximum number of bytes we can append to this array.
     */
    [[nodiscard]] virtual size_t max_bytes() const = 0;

  protected:
    std::shared_ptr<ArrayConfig> config_;
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3Client> s3_client_;
    std::shared_ptr<FileHandlePool> file_handle_pool_;

    // JSON metadata
    const std::string metadata_path_{ "zarr.json" };
    std::mutex metadata_mutex_;
    std::unordered_map<std::string, nlohmann::json> custom_metadata_;
    std::unique_ptr<Sink> metadata_sink_;

    std::string node_path_() const;
    [[nodiscard]] virtual bool make_metadata_(nlohmann::json& metadata) = 0;
    [[nodiscard]] bool make_metadata_sink_();
    [[nodiscard]] bool write_metadata_();

    /**
     * @brief Close the node and flush any remaining data.
     * @return True if the node was closed successfully, false otherwise.
     */
    [[nodiscard]] virtual bool close_() = 0;

    friend bool finalize_array(std::unique_ptr<ArrayBase>&& array);
};

[[nodiscard]] bool
finalize_array(std::unique_ptr<ArrayBase>&& array);
} // namespace zarr
