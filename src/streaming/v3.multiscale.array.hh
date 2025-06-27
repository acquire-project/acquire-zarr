#pragma once

#include "multiscale.array.hh"
#include "v3.array.hh"

namespace zarr {
class V3MultiscaleArray final : public MultiscaleArray
{
  public:
    V3MultiscaleArray(std::shared_ptr<ArrayConfig> config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;

    bool create_arrays_() override;
    nlohmann::json get_ome_metadata_() const override;
};
} // namespace zarr