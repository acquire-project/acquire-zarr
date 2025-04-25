#pragma once

#include "array.writer.hh"

namespace zarr {
class ZarrGroup
{
  public:
    ZarrGroup();
    ~ZarrGroup();

  private:
    std::vector<std::shared_ptr<ArrayWriter>> arrays_;
};
} // namespace zarr