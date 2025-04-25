#pragma once

#include "group.hh"
#include "v2.array.hh"

namespace zarr {
class V2Group final : public Group
{
  public:
    V2Group(const GroupConfig& config, std::shared_ptr<ThreadPool> thread_pool);
    V2Group(const GroupConfig& config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    bool create_arrays_() override;
    nlohman::json make_ome_metadata_() const override;
};
} // namespace zarr