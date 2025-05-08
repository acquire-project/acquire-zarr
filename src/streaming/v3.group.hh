#pragma once

#include "group.hh"
#include "v3.array.hh"

namespace zarr {
class V3Group final : public Group
{
  public:
    V3Group(std::shared_ptr<GroupConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    std::string get_metadata_key() const override;

    nlohmann::json get_ome_metadata() const override;

  private:
    nlohmann::json make_group_metadata_() const override;

    bool create_arrays_() override;
};
} // namespace zarr