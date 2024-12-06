#pragma once

#include "array.writer.hh"
#include "shard.writer.hh"

namespace zarr {
struct ZarrV3ArrayWriter : public ArrayWriter
{
  public:
    ZarrV3ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool);
    ZarrV3ArrayWriter(
      const ArrayWriterConfig& config,
      std::shared_ptr<ThreadPool> thread_pool,
      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<std::unique_ptr<ShardWriter>> shards_ready_;

    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;
    std::vector<std::vector<uint32_t>> chunk_in_shards_;

    std::string data_root_() const override;
    std::string metadata_path_() const override;
    PartsAlongDimensionFun parts_along_dimension_() const override;
    void compress_and_flush_() override;
    bool flush_impl_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
