#pragma once

#include "multiscale.array.hh"
#include "s3.array.hh"
#include "s3.storage.hh"

namespace zarr {
class S3MultiscaleArray
  : public MultiscaleArray
  , public S3Storage
{
  public:
    S3MultiscaleArray(std::shared_ptr<ArrayConfig> config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  protected:
    bool write_metadata_() override;

    bool create_arrays_() override;
};
} // namespace zarr