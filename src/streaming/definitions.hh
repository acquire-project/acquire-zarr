#pragma once

#include <cstdint> // uint8_t
#include <span>
#include <vector>

using ByteVector = std::vector<uint8_t>;
using BytePtr = uint8_t*;
using ConstBytePtr = const BytePtr;

using ByteSpan = std::span<uint8_t>;
using ConstByteSpan = std::span<const uint8_t>;

constexpr int MAX_CONCURRENT_FILES = 256;
