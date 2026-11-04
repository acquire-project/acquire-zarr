#pragma once

#include <atomic>
#include <cstdint> // uint8_t
#include <span>
#include <vector>

using ByteVector = std::vector<uint8_t>;

using ByteSpan = std::span<uint8_t>;
using ConstByteSpan = std::span<const uint8_t>;

/** Bytes given to the shard sinks of a stream: chunk data and shard indexes. */
using ByteCounter = std::atomic<uint64_t>;
