#pragma once

#include "array.writer.hh"

namespace zarr {
struct ZarrV3ArrayWriter : public ArrayWriter
{
  public:
    ZarrV3ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool);
    ZarrV3ArrayWriter(const ArrayWriterConfig& config,
                      std::shared_ptr<ThreadPool> thread_pool,
                      std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;
    uint32_t current_layer_;

    size_t compute_chunk_offsets_and_defrag_(uint32_t shard_index);

    std::string data_root_() const override;
    std::string metadata_path_() const override;
    const DimensionPartsFun parts_along_dimension_() const override;
    void make_buffers_() override;
    BytePtr get_chunk_data_(uint32_t index) override;
    bool compress_and_flush_data_() override;
    bool write_array_metadata_() override;
    bool should_rollover_() const override;
};
} // namespace zarr
