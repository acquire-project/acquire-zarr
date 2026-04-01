#pragma once

#include "blosc.compression.params.hh"
#include "zstd.compression.params.hh"

#include <variant>

namespace zarr {
using CompressionParams =
  std::variant<BloscCompressionParams, ZstdCompressionParams>;
} // namespace zarr
