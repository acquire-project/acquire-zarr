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
    std::mutex mutex_;
    size_t table_size_;
    std::unordered_map<std::string, std::mutex> shard_mutexes_;
    std::unordered_map<std::string, std::vector<std::future<void>>> futures_;
    std::unordered_map<std::string, std::shared_ptr<void>> handles_;

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