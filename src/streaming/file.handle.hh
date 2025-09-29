#pragma once

#include <condition_variable>
#include <memory>  // for std::unique_ptr
#include <cstddef> // for std::size_t
#include <mutex>
#include <string>

namespace zarr {
class FileHandle
{
  public:
    FileHandle(const std::string& filename, void* flags);
    ~FileHandle();

    void* get() const;

  private:
    void* handle_;
};

class FileHandlePool
{
  public:
    FileHandlePool();
    ~FileHandlePool() = default;

    std::unique_ptr<FileHandle> get_handle(const std::string& filename,
                                           void* flags);
    void return_handle(std::unique_ptr<FileHandle>&& handle);

  private:
    const uint64_t max_active_handles_;
    std::atomic<uint64_t> n_active_handles_;
    std::mutex mutex_;
    std::condition_variable cv_;
};
} // namespace zarr