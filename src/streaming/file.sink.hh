#pragma once

#include "file.handle.hh"
#include "sink.hh"

#include <fstream>
#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    FileSink(std::string_view filename,
             std::shared_ptr<FileHandlePool> file_handle_pool);
    ~FileSink() override;

    bool write(size_t offset, ConstByteSpan data) override;

  protected:
    bool flush_() override;

  private:
    std::shared_ptr<FileHandlePool> file_handle_pool_;

    std::string filename_;
    void* flags_;
};
} // namespace zarr
