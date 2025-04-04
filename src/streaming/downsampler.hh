#pragma once

#include "definitions.hh"
#include "zarr.dimension.hh"
#include "array.writer.hh"

#include <unordered_map>

namespace zarr {
class Downsampler
{
  public:
    Downsampler(const ArrayWriterConfig& config,
                std::shared_ptr<ThreadPool> thread_pool,
                std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    void add_frame(ConstByteSpan frame_data);

  private:
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    ArrayWriterConfig array_writer_config_;
    std::unordered_map<int, std::unique_ptr<ArrayWriter>> writers_;

    bool is_3d_downsample_() const;
    std::vector<ArrayWriterConfig> make_downsampled_configs_();
    void make_writers_();
};
} // namespace zarr