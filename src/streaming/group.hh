#pragma once

#include "array.hh"

namespace zarr {
struct GroupConfig
{
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
    std::optional<std::string> bucket_name;
    std::string store_root; /**< Path to the root of the store, e.g., my-dataset.zarr */
    std::optional<BloscCompressionParams> compression_params;
};

class Group
{
  public:
    Group() = default;
    ~Group() = default;

  private:
    std::vector<std::shared_ptr<Array>> arrays_;
};
} // namespace zarr