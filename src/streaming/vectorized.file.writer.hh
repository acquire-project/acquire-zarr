#pragma once

#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#endif

namespace zarr {
class VectorizedFileWriter
{
  public:
    explicit VectorizedFileWriter(const std::string& path);
    ~VectorizedFileWriter();

    bool write_vectors(const std::vector<std::span<std::byte>>& buffers,
                       size_t offset);

    std::mutex& mutex() { return mutex_; }

  private:
    std::mutex mutex_;
    size_t page_size_;
#ifdef _WIN32
    HANDLE handle_;
    size_t sector_size_;
#else
    int fd_;
#endif

    size_t align_size_(size_t size) const;
    size_t align_to_page_(size_t size) const;
};
} // namespace zarr