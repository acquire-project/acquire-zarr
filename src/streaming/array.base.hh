#pragma once

#include "array.dimensions.hh"
#include "blosc.compression.params.hh"
#include "definitions.hh"
#include "locked.buffer.hh"
#include "s3.connection.hh"
#include "sink.hh"
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
              std::shared_ptr<ThreadPool> thread_pool,
              std::shared_ptr<S3ConnectionPool> s3_connection_pool);
    virtual ~ArrayBase() = default;

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
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    std::unordered_map<std::string, std::string> metadata_strings_;
    std::unordered_map<std::string, std::unique_ptr<Sink>> metadata_sinks_;

    std::string node_path_() const;
    [[nodiscard]] virtual bool make_metadata_() = 0;
    virtual std::vector<std::string> metadata_keys_() const = 0;
    [[nodiscard]] bool make_metadata_sinks_();
    [[nodiscard]] bool write_metadata_();

    friend bool finalize_node(std::unique_ptr<ArrayBase>&& node);
};

std::unique_ptr<ArrayBase>
make_array(std::shared_ptr<zarr::ArrayConfig> config,
           std::shared_ptr<ThreadPool> thread_pool,
           std::shared_ptr<S3ConnectionPool> s3_connection_pool,
           ZarrVersion format);

template<class T>
std::unique_ptr<T>
downcast_node(std::unique_ptr<ArrayBase>&& node)
{
    ArrayBase* raw_ptr = node.release();
    T* derived_ptr = dynamic_cast<T*>(raw_ptr);

    if (!derived_ptr) {
        node.reset(raw_ptr);
        return nullptr;
    }

    return std::unique_ptr<T>(derived_ptr);
}

[[nodiscard]] bool
finalize_node(std::unique_ptr<ArrayBase>&& node);
} // namespace zarr