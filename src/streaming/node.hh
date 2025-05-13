#pragma once

#include "array.dimensions.hh"
#include "blosc.compression.params.hh"
#include "definitions.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "thread.pool.hh"
#include "zarr.types.h"

#include <string>

namespace zarr {
struct ZarrNodeConfig
{
    ZarrNodeConfig() = default;
    ZarrNodeConfig(std::string_view store_root,
                   std::string_view group_key,
                   std::optional<std::string> bucket_name,
                   std::optional<BloscCompressionParams> compression_params,
                   std::shared_ptr<ArrayDimensions> dimensions,
                   ZarrDataType dtype)
      : store_root(store_root)
      , group_key(group_key)
      , bucket_name(bucket_name)
      , compression_params(compression_params)
      , dimensions(std::move(dimensions))
      , dtype(dtype)
    {
    }

    virtual ~ZarrNodeConfig() = default;

    std::string store_root;
    std::string group_key;
    std::optional<std::string> bucket_name;
    std::optional<BloscCompressionParams> compression_params;
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
};

class ZarrNode
{
  public:
    ZarrNode(std::shared_ptr<ZarrNodeConfig> config,
             std::shared_ptr<ThreadPool> thread_pool,
             std::shared_ptr<S3ConnectionPool> s3_connection_pool);
    virtual ~ZarrNode() = default;

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
    [[nodiscard]] virtual size_t write_frame(ConstByteSpan data) = 0;

  protected:
    std::shared_ptr<ZarrNodeConfig> config_;
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    std::unordered_map<std::string, std::string> metadata_strings_;
    std::unordered_map<std::string, std::unique_ptr<Sink>> metadata_sinks_;

    virtual std::string node_path_() const = 0;
    [[nodiscard]] virtual bool make_metadata_() = 0;
    virtual std::vector<std::string> metadata_keys_() const = 0;
    [[nodiscard]] bool make_metadata_sinks_();
    [[nodiscard]] bool write_metadata_();

    friend bool finalize_node(std::unique_ptr<ZarrNode>&& node);
};

template<class T>
std::unique_ptr<T>
downcast_node(std::unique_ptr<ZarrNode>&& node)
{
    ZarrNode* raw_ptr = node.release();
    T* derived_ptr = dynamic_cast<T*>(raw_ptr);

    if (!derived_ptr) {
        node.reset(raw_ptr);
        return nullptr;
    }

    return std::unique_ptr<T>(derived_ptr);
}

[[nodiscard]] bool
finalize_node(std::unique_ptr<ZarrNode>&& node);
} // namespace zarr