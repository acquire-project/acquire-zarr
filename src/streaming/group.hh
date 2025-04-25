#pragma once

#include "array.hh"
#include "thread.pool.hh"

namespace zarr {
struct GroupConfig
{
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
    std::optional<std::string> bucket_name;
    std::string
      store_root; /**< Path to the root of the store, e.g., my-dataset.zarr */
    std::optional<BloscCompressionParams> compression_params;
};

class Group
{
  public:
    Group(const GroupConfig& config, std::shared_ptr<ThreadPool> thread_pool);
    Group(const GroupConfig& config,
          std::shared_ptr<ThreadPool> thread_pool,
          std::shared_ptr<S3ConnectionPool> s3_connection_pool);
    virtual ~Group() = default;

  private:
    GroupConfig config_;

    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    std::vector<std::shared_ptr<Array>> arrays_;
};
} // namespace zarr