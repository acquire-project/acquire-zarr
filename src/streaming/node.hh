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
struct NodeConfig
{
    NodeConfig() = default;
    NodeConfig(std::string_view store_root,
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

    virtual ~NodeConfig() = default;

    std::string store_root;
    std::string group_key;
    std::optional<std::string> bucket_name;
    std::optional<BloscCompressionParams> compression_params;
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
};

class Node
{
  public:
    Node(std::shared_ptr<NodeConfig> config,
         std::shared_ptr<ThreadPool> thread_pool,
         std::shared_ptr<S3ConnectionPool> s3_connection_pool);
    virtual ~Node() = default;

    /**
     * @brief Open the node for writing.
     * @return True if the node was opened successfully, false otherwise.
     */
    [[nodiscard]] virtual bool open() = 0;

    /**
     * @brief Close the node and flush any remaining data.
     * @note The node may be reopened after closing, but only the metadata will
     * be preserved. Do not close unless you really want to flush all data and
     * metadata.
     * @return True if the node was closed successfully, false otherwise.
     */
    [[nodiscard]] virtual bool close() = 0;

    /**
     * @brief Write a frame of data to the node.
     * @param data The data to write.
     * @return The number of bytes successfully written.
     */
    [[nodiscard]] virtual size_t write_frame(ConstByteSpan data) = 0;

    /**
     * @brief Get the metadata key for the node.
     * @return The metadata key for the node.
     */
    [[nodiscard]] virtual std::string get_metadata_key() const = 0;

  protected:
    std::shared_ptr<NodeConfig> config_;
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    bool is_open_;
    std::unique_ptr<Sink> metadata_sink_;
};
} // namespace zarr