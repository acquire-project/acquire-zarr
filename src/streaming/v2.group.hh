#pragma once

#include "group.hh"
#include "v2.array.hh"

namespace zarr {
class V2Group final : public Group
{
  public:
    V2Group(std::shared_ptr<GroupConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;

    bool create_arrays_() override;
    nlohmann::json get_ome_metadata_() const override;
};
} // namespace zarr