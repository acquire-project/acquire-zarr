#pragma once

#include "file.handle.hh"
#include "s3.connection.hh"
#include "thread.pool.hh"

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace zarr {
class ResourcePool
{
  public:
    explicit ResourcePool(size_t max_threads);

    const std::optional<std::string>& error() const;
    uint64_t active_file_handles();
    uint64_t active_s3_connections();
    uint64_t memory_usage();

    std::shared_ptr<ThreadPool> thread_pool() const;

    std::shared_ptr<FileHandle> get_file_handle(const std::string& path,
                                                void* flags);

    std::shared_ptr<S3Connection> get_s3_connection(
      const std::string& object_key,
      const S3Settings& s3_settings);

    using DataBuffer = std::vector<uint8_t>;
    std::shared_ptr<DataBuffer> get_data_buffer(uint64_t bytes);

  private:
    struct BufferContainer
    {
        std::weak_ptr<DataBuffer> data;
        uint64_t bytes;
    };

    const uint64_t max_active_files_;
    const uint64_t max_memory_bytes_;
    uint64_t active_memory_bytes_;

    std::shared_ptr<ThreadPool> thread_pool_;

    mutable std::mutex files_mutex_;
    std::condition_variable files_cv_;
    std::unordered_map<std::string, std::weak_ptr<FileHandle>> files_;

    mutable std::mutex s3_connections_mutex_;
    std::condition_variable s3_connections_cv_;
    std::unordered_map<std::string, std::weak_ptr<S3Connection>>
      s3_connections_;

    mutable std::mutex buffers_mutex_;
    std::condition_variable buffers_cv_;
    std::vector<BufferContainer> buffers_;

    std::optional<std::string> error_;

    void set_error_(const std::string& error);

    void cull_unused_files_();
    void cull_unused_s3_connections_();
    void cull_unused_buffers_();
};
} // namespace zarr

struct ZarrResourcePool_s
{
    explicit ZarrResourcePool_s(unsigned int max_threads);

    std::shared_ptr<zarr::ResourcePool> inner() const;

  private:
    std::shared_ptr<zarr::ResourcePool> inner_;
};