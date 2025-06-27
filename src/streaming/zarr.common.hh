#pragma once

#include "acquire.zarr.h"
#include "thread.pool.hh"
#include "array.dimensions.hh"
#include "definitions.hh"
#include "blosc.compression.params.hh"

namespace zarr {
/**
 * @brief Trim whitespace from a string.
 * @param s The string to trim.
 * @return The string with leading and trailing whitespace removed.
 */
[[nodiscard]]
std::string
trim(std::string_view s);

/**
 * @brief Check if a string is empty, including whitespace.
 * @param s The string to check.
 * @param err_on_empty The message to log if the string is empty.
 * @return True if the string is empty, false otherwise.
 */
bool
is_empty_string(std::string_view s, std::string_view err_on_empty);

/**
 * @brief Get the number of bytes for a given data type.
 * @param data_type The data type.
 * @return The number of bytes for the data type.
 * @throw std::invalid_argument if the data type is not recognized.
 */
size_t
bytes_of_type(ZarrDataType data_type);

/**
 * @brief Get the number of bytes for a frame with the given dimensions and
 * data type.
 * @param dims The dimensions of the full array.
 * @param type The data type of the array.
 * @return The number of bytes for a single frame.
 * @throw std::invalid_argument if the data type is not recognized.
 */
size_t
bytes_of_frame(const ArrayDimensions& dims, ZarrDataType type);

/**
 * @brief Get the number of chunks along a dimension.
 * @param dimension A dimension.
 * @return The number of, possibly ragged, chunks along the dimension, given
 * the dimension's array and chunk sizes.
 * @throw std::runtime_error if the chunk size is zero.
 */
uint32_t
chunks_along_dimension(const ZarrDimension& dimension);

/**
 * @brief Get the number of shards along a dimension.
 * @param dimension A dimension.
 * @return The number of shards along the dimension, given the dimension's
 * array, chunk, and shard sizes.
 */
uint32_t
shards_along_dimension(const ZarrDimension& dimension);
} // namespace zarr