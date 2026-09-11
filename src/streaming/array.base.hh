#pragma once

#include "array.dimensions.hh"
#include "compression.params.hh"
#include "file.handle.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "thread.pool.hh"
#include "zarr.types.h"

#include <nlohmann/json.hpp>

#include <string>

namespace zarr {
// OME-NGFF version string emitted in the `ome.version` metadata field.
inline std::string
ome_version_to_string(ZarrOMEVersion version)
{
    switch (version) {
        case ZarrOMEVersion_0_6:
            return "0.6";
        case ZarrOMEVersion_0_5:
        default:
            return "0.5";
    }
}

// Internal representation of one OME "omero" rendering channel, copied out of
// the transient ZarrOMEChannel C struct at commit time.
struct OMEChannel
{
    std::optional<std::string> label;
    std::optional<std::string> color;
    ZarrOMEWindow window;
    bool active{ false };
    std::optional<std::string> family;
    std::optional<double> coefficient;
    bool inverted{ false };
};

// Internal representation of OME "omero" rendering metadata for one image.
struct OMERendering
{
    std::optional<uint32_t> id;
    std::optional<std::string> name;
    std::vector<OMEChannel> channels;
    bool has_rdefs{ false };
    std::optional<std::string> model;
    uint32_t default_t{ 0 };
    uint32_t default_z{ 0 };
};

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
                uint32_t max_levels = 0)
      : store_root(store_root)
      , node_key(group_key)
      , bucket_name(bucket_name)
      , compression_params(compression_params)
      , dimensions(std::move(dimensions))
      , dtype(dtype)
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
    std::optional<ZarrDownsamplingMethod> downsampling_method;
    uint16_t level_of_detail;
    uint32_t max_levels{ 0 };

    // OME-NGFF metadata version to emit for this node.
    ZarrOMEVersion ome_version{ ZarrOMEVersion_0_5 };
    // Optional OME "omero" rendering metadata for this image.
    std::optional<OMERendering> omero;
};

enum class WriteResult
{
    Ok,
    PartialWrite,
    OutOfBounds,
    FrameSizeMismatch,
    FrameOutOfOrder, // frame ID gap detected; predecessor not yet written
};

class ArrayBase
{
  public:
    ArrayBase(std::shared_ptr<ArrayConfig> config,
              std::shared_ptr<ThreadPool> thread_pool,
              std::shared_ptr<FileHandlePool> file_handle_pool,
              std::shared_ptr<S3ConnectionPool> s3_connection_pool);
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
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;
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

std::unique_ptr<ArrayBase>
make_array(std::shared_ptr<ArrayConfig> config,
           std::shared_ptr<ThreadPool> thread_pool,
           std::shared_ptr<FileHandlePool> file_handle_pool,
           std::shared_ptr<S3ConnectionPool> s3_connection_pool,
           bool is_hcs_array);

[[nodiscard]] bool
finalize_array(std::unique_ptr<ArrayBase>&& array);
} // namespace zarr