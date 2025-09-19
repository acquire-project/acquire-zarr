#pragma once

#include "sink.hh"

#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    explicit FileSink(std::string_view filename);
    ~FileSink() override;

    bool write(size_t offset, ConstByteSpan data) override;
    bool write(size_t& offset,
               const std::vector<std::vector<uint8_t>>& buffers) override;

  protected:
    bool flush_() override;

  private:
    void* handle_;         // platform-specific file handle
    std::string filename_; // keep a copy of the filename for reopening
    bool vectorized_;      // whether to use vectorized writes
    size_t sector_size_;   // cached system sector size
    std::mutex mutex_;
};
} // namespace zarr
