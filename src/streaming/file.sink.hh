#pragma once

#include "file.handle.hh"
#include "sink.hh"

#include <fstream>
#include <mutex>
#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    FileSink(std::string_view filename,
             std::shared_ptr<FileHandlePool> file_handle_pool,
             bool truncate_to_fit = false);
    ~FileSink() override;

    bool write(size_t offset, ConstByteSpan data) override;

  protected:
    bool flush_() override;

  private:
    std::shared_ptr<FileHandlePool> file_handle_pool_;

    std::string filename_;

    // Whole-file replacement: drop bytes past what was written so a shorter
    // rewrite leaves no stale tail. Used for metadata (single write at 0).
    bool truncate_to_fit_;
};
} // namespace zarr
