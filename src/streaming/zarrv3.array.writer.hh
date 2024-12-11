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
    std::vector<std::unique_ptr<ShardWriter>> shard_writers_;

    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;
    std::vector<std::vector<uint32_t>> chunk_in_shards_;

    std::string data_root_() const override;
    std::string metadata_path_() const override;
    bool make_data_sinks_() override;
    bool should_rollover_() const override;
    void compress_and_flush_() override;
    void close_sinks_() override;
    bool write_array_metadata_() override;

    bool compress_and_flush_to_filesystem_();
    bool compress_and_flush_to_s3_();
};
} // namespace zarr
