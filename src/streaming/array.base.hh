#pragma once

#include "array.dimensions.hh"
#include "blosc.compression.params.hh"
#include "locked.buffer.hh"
#include "thread.pool.hh"
#include "zarr.types.h"

#include <string>

namespace zarr {
struct ArrayConfig
{
    ArrayConfig() = default;
    ArrayConfig(std::string_view store_root,
                std::string_view group_key,
                std::optional<std::string> bucket_name,
                std::optional<BloscCompressionParams> compression_params,
                std::shared_ptr<ArrayDimensions> dimensions,
                ZarrDataType dtype,
                std::optional<ZarrDownsamplingMethod> downsampling_method,
                uint16_t level_of_detail)
      : store_root(store_root)
      , node_key(group_key)
      , bucket_name(bucket_name)
      , compression_params(compression_params)
      , dimensions(std::move(dimensions))
      , dtype(dtype)
      , downsampling_method(downsampling_method)
      , level_of_detail(level_of_detail)
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
    std::optional<BloscCompressionParams> compression_params;
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
    std::optional<ZarrDownsamplingMethod> downsampling_method;
    uint16_t level_of_detail;
};

class ArrayBase
{
  public:
    ArrayBase(std::shared_ptr<ArrayConfig> config,
              std::shared_ptr<ThreadPool> thread_pool);
    virtual ~ArrayBase() = default;

    /**
     * @brief Get the amount of memory currently used by this Array, in bytes.
     * @return Memory used by this object, in bytes.
     */
    virtual size_t memory_usage() const noexcept = 0;

    /**
     * @brief Close the node and flush any remaining data.
     * @return True if the node was closed successfully, false otherwise.
     */
    [[nodiscard]] virtual bool close_() = 0;

    /**
     * @brief Write a frame of data to the node.
     * @param data The data to write.
     * @return The number of bytes successfully written.
     */
    [[nodiscard]] virtual size_t write_frame(LockedBuffer& data) = 0;

  protected:
    std::shared_ptr<ArrayConfig> config_;
    std::shared_ptr<ThreadPool> thread_pool_;

    std::string last_written_metadata_;

    std::string node_path_() const;
    [[nodiscard]] virtual bool make_metadata_(std::string& metadata_str) = 0;
    [[nodiscard]] virtual bool write_metadata_() = 0;

    friend bool finalize_array(std::unique_ptr<ArrayBase>&& array);
};

template<typename ArrayType, typename... Args>
std::unique_ptr<ArrayBase>
make_array(std::shared_ptr<ArrayConfig> config,
           std::shared_ptr<ThreadPool> thread_pool,
           Args&&... args)
{
    return std::make_unique<ArrayType>(
      config, thread_pool, std::forward<Args>(args)...);
}

[[nodiscard]] bool
finalize_array(std::unique_ptr<ArrayBase>&& array);
} // namespace zarr