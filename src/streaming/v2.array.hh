#pragma once

#include "array.hh"

namespace zarr {
class V2Array final : public Array
{
  public:
    V2Array(const ArrayConfig& config, std::shared_ptr<ThreadPool> thread_pool);
    V2Array(const ArrayConfig& config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::string data_root_() const override;
    std::string metadata_path_() const override;
    const DimensionPartsFun parts_along_dimension_() const override;
    void make_buffers_() override;
    BytePtr get_chunk_data_(uint32_t index) override;
    bool compress_and_flush_data_() override;
    bool write_array_metadata_() override;
    void close_sinks_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
