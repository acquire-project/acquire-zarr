#pragma once

#include <span>
#include <vector>

using ByteVector = std::vector<std::byte>;

using ByteSpan = std::span<std::byte>;
using ConstByteSpan = std::span<const std::byte>;
