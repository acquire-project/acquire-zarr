#pragma once

#include "file.handle.hh"
#include "sink.hh"

#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    FileSink(std::string_view filename,
             std::shared_ptr<FileHandlePool> file_handle_pool);
    ~FileSink() override;

    bool write(size_t offset, ConstByteSpan data) override;
    bool write(size_t offset,
               const std::vector<std::vector<uint8_t>>& buffers) override;
    size_t align_to_system_size(size_t size) override;

  protected:
    bool flush_() override;

  private:
    std::shared_ptr<FileHandlePool> file_handle_pool_;
    std::string filename_; // keep a copy of the filename for reopening

    void* flags_;          // platform-specific flags for opening the file
    void* handle_;         // platform-specific file handle
    bool vectorized_;      // whether to use vectorized writes
    size_t page_size_;     // cached system page size
    size_t sector_size_;   // cached system sector size
};
} // namespace zarr
