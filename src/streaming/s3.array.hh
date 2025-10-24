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
    std::string index_location_() const override;
    bool compress_and_flush_data_() override;
    void finalize_io_streams_() override;

    /**
     * @brief Compress all the chunk buffers in place.
     * @return True on success, false on failure.
     */
    bool compress_chunks_();

    /**
     * @brief Update the shard tables with the sizes of the compressed chunks.
     */
    void update_table_entries_();

    /**
     * @brief Collect all the chunks for a given shard index in the current
     * layer.
     * @param shard_index The index of the shard to collect chunks for.
     * @return The collected chunk buffers in a shard layer structure.
     */
    ShardLayer collect_chunks_(uint32_t shard_index);

    /**
     * @brief Flush the chunk data to S3 or intermediate buffers.
     * @return True on success, false on failure.
     */
    bool flush_data_();

    /**
     * @brief Flush all shard tables to S3.
     * @return True on success, false on failure.
     */
    bool flush_tables_();
};
} // namespace zarr