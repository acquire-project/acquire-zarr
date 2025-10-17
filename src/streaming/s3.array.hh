#pragma once

#include "array.hh"
#include "s3.storage.hh"

namespace zarr {
class S3Array final
  : public Array
  , public S3Storage
{
  public:
    S3Array(std::shared_ptr<ArrayConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  protected:
    bool write_metadata_() override;

    bool flush_data_() override;
    bool flush_tables_() override;
    void close_io_streams_() override;
};
} // namespace zarr