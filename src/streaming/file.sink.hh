#pragma once

#include "sink.hh"

#include <fstream>
#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    FileSink(std::string_view filename);
    ~FileSink() override;

    bool write(size_t offset, ConstByteSpan data) override;

  protected:
    bool flush_() override;

  private:
    std::mutex mutex_;
    size_t page_size_;
    size_t sector_size_;

    void* handle_;
};
} // namespace zarr
