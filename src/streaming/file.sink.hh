#pragma once

#include "sink.hh"

#include <fstream>
#include <string_view>

namespace zarr {
class FileSink : public Sink
{
  public:
    explicit FileSink(std::string_view filename);

    bool write(size_t offset, ConstByteSpan data) override;

  protected:
    bool flush_() override;

  private:
    std::ofstream file_;
};
} // namespace zarr
