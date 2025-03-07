#pragma once

#include "sink.hh"

#include <fstream>
#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    FileSink(std::string_view filename, bool truncate = true);

    bool write(size_t offset, std::span<const std::byte> data) override;

  protected:
    bool flush_() override;

  private:
    std::ofstream file_;
};
} // namespace zarr
