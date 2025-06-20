#pragma once

#include "array.hh"

namespace zarr {
class V2Array final : public Array
{
  public:
    V2Array(std::shared_ptr<ArrayConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;

    std::string data_root_() const override;
    const DimensionPartsFun parts_along_dimension_() const override;
    bool compress_and_flush_data_() override;
    void close_sinks_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
