#pragma once

#include "array.writer.hh"

namespace zarr {
class ZarrV2ArrayWriter final : public ArrayWriter
{
  public:
    ZarrV2ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool);

    ZarrV2ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::string data_root_() const override;
    std::string metadata_path_() const override;
    bool make_data_sinks_() override;
    bool should_rollover_() const override;
    void compress_and_flush_() override;
    bool write_array_metadata_() override;
};
} // namespace zarr
