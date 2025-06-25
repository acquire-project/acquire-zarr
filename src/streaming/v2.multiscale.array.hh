#pragma once

#include "multiscale.array.hh"
#include "v2.array.hh"

namespace zarr {
class V2MultiscaleArray final : public MultiscaleArray
{
  public:
    V2MultiscaleArray(std::shared_ptr<ArrayConfig> config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;

    bool create_arrays_() override;
    nlohmann::json get_ome_metadata_() const override;
};
} // namespace zarr