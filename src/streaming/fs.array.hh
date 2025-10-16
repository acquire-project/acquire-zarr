#pragma once

#include "array.hh"
#include "fs.storage.hh"

namespace zarr {
class FSArray final
  : public Array
  , public FSStorage
{
  public:
    FSArray(std::shared_ptr<ArrayConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<FileHandlePool> file_handle_pool);

  protected:
    bool write_metadata_() override;

    bool flush_data_() override;
    bool flush_tables_() override;
    void close_io_streams_() override;
};
} // namespace zarr