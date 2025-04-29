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

    nlohmann::json get_ome_metadata() const override;

  private:
    std::string get_metadata_key_() const override;
    nlohmann::json make_group_metadata_() const override;

    bool create_arrays_() override;
};
} // namespace zarr