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
#include <unordered_map>

namespace zarr {
/**
 * @brief Merge attributes that the caller gives into a node's attributes.
 * @details With an empty @p key, each top-level key of @p value, which must be
 * an object, becomes an
 * attribute. Otherwise @p value is stored under @p key. Shared by arrays
 * and groups so that every node applies the same rules for `ome`:
 * - `ome` must be a JSON object, given with @p key `ome` or as a top-level key
 *   of @p value. A @p key that begins with `ome/` is refused, because a key
 *   is not a path.
 * - `ome.multiscales` is refused on every node. The stream writes it.
 * - A key that the stream writes under `ome` on this node (@p library_ome) is
 *   refused, except `version` with the same value.
 * @param[in, out] attributes The attributes that the caller gave the node.
 * @param key The attribute key, or empty.
 * @param value The JSON value to merge. With an empty @p key, it must be an
 * object.
 * @param library_ome The object that the stream writes under `ome` on this
 * node, or null if it writes none.
 * @return True on success, false if a rule above refuses the value.
 */
[[nodiscard]] bool
merge_attributes(std::unordered_map<std::string, nlohmann::json>& attributes,
                 const std::string& key,
                 const nlohmann::json& value,
                 const nlohmann::json& library_ome);

/**
 * @brief Write the attributes that the caller gave a node into the node's
 * `attributes` object.
 * @details `ome` is merged key by key into the `ome` already in
 * @p attributes; any other key replaces what is there.
 */
void
apply_attributes(
  nlohmann::json& attributes,
  const std::unordered_map<std::string, nlohmann::json>& user_attributes);

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
     * @brief Write attributes to the array.
     * @param key The key under 'attributes' in the array metadata, or empty
     * to write each key of @p metadata there.
     * @param metadata The JSON value to write.
     * @return True on success, false on failure.
     */
    bool write_attributes(const std::string& key,
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
    std::unordered_map<std::string, nlohmann::json> attributes_;
    std::unique_ptr<Sink> metadata_sink_;

    std::string node_path_() const;
    [[nodiscard]] virtual bool make_metadata_(nlohmann::json& metadata) = 0;

    /**
     * @brief The object that this node writes under `ome`, or null if it
     * writes none.
     */
    [[nodiscard]] virtual nlohmann::json library_ome_metadata_() const;
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
