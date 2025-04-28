#pragma once

#include "group.hh"
#include "v3.array.hh"

namespace zarr {
class V3Group final : public Group
{
  public:
    V3Group(const GroupConfig& config, std::shared_ptr<ThreadPool> thread_pool);
    V3Group(const GroupConfig& config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    nlohmann::json get_ome_metadata() const override;

  private:
    bool create_arrays_() override;
};
} // namespace zarr