#pragma once

#include "array.hh"

#include <latch>

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

    std::unordered_map<std::string, std::unique_ptr<Sink>> data_sinks_;

    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;

    std::string data_root_() const override;
    const DimensionPartsFun parts_along_dimension_() const override;
    bool compress_and_flush_data_() override;
    void close_sinks_() override;
    bool should_rollover_() const override;

    ByteVector consolidate_chunks_(uint32_t shard_index);
};
} // namespace zarr
