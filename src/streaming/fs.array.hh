#pragma once

#include "array.hh"
#include "fs.storage.hh"

#include <future>
#include <unordered_map>

namespace zarr {
class FSArray final
  : public Array
  , public FSStorage
{
  public:
    FSArray(std::shared_ptr<ArrayConfig> config,
            std::shared_ptr<ThreadPool> thread_pool,
            std::shared_ptr<FileHandlePool> file_handle_pool);

  protected:
    struct ShardFile
    {
        std::string path;
        std::shared_ptr<void> handle;
        std::vector<uint64_t> table;
        std::mutex table_mutex;
        uint64_t file_offset;
        std::mutex offset_mutex;
        std::vector<std::future<void>> chunk_futures;

        [[nodiscard]] bool close();
    };

    const size_t table_size_bytes_;

    std::unordered_map<std::string, std::shared_ptr<ShardFile>> shard_files_;
    std::mutex shard_files_mutex_;
    std::condition_variable shard_files_cv_;

    std::unordered_map<std::string, std::shared_ptr<void>> handles_;
    std::mutex handles_mutex_;

    bool write_metadata_() override;
    std::string index_location_() const override;
    bool compress_and_flush_data_() override;
    void finalize_append_shard_() override;

    /**
     * @brief Get a file handle for the given path, creating it and adding it to
     * the local handle pool if it does not already exist.
     * @param path The file path.
     * @return The file handle.
     */
    std::shared_ptr<void> get_handle_(const std::string& path);

    /**
     * @brief Write the shard table entries for the given shard index.
     * @param shard_idx The shard index.
     */
    void write_table_entries_(uint32_t shard_idx);
};
} // namespace zarr