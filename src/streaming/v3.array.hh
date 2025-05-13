#pragma once

#include "array.hh"

namespace zarr {
class V3Array final : public Array
{
  public:
    V3Array(std::shared_ptr<ArrayConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<S3ConnectionPool> s3_connection_pool);

  private:
    std::vector<size_t> shard_file_offsets_;
    std::vector<std::vector<uint64_t>> shard_tables_;
    uint32_t current_layer_;

    std::unordered_map<std::string, std::unique_ptr<Sink>> s3_data_sinks_;

    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;

    std::string data_root_() const override;
    const DimensionPartsFun parts_along_dimension_() const override;
    void make_buffers_() override;
    BytePtr get_chunk_data_(uint32_t index) override;
    bool compress_and_flush_data_() override;
    void close_sinks_() override;
    bool should_rollover_() const override;

    size_t compute_chunk_offsets_and_defrag_(uint32_t shard_index);
};
} // namespace zarr
