#pragma once

#include "fs.array.hh"
#include "fs.storage.hh"
#include "multiscale.array.hh"

namespace zarr {
class FSMultiscaleArray
  : public MultiscaleArray
  , public FSStorage
{
  public:
    FSMultiscaleArray(std::shared_ptr<ArrayConfig> config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<FileHandlePool> file_handle_pool);

  protected:
    bool write_metadata_() override;

    bool create_arrays_() override;
};
} // namespace zarr