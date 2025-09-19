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
 * @param array_size Size of the array along the dimension, in pixels.
 * @param part_size Size of the chunks along the dimension, in pixels.
 * @return The number of, possibly ragged, chunks along the dimension.
 */
uint32_t
parts_along_dimension(uint32_t array_size, uint32_t part_size);

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

/**
 * @brief Compress @p data in place using Blosc with the given parameters.
 * @param data The buffer to compress.
 * @param params Compression parameters.
 * @param type_size Size of the data type being compressed (e.g., 1 for uint8, 2
 * for uint16).
 * @return true if compression was successful, false otherwise.
 */
bool
compress_in_place(ByteVector& data,
                  const BloscCompressionParams& params,
                  size_t type_size);

size_t
align_to(size_t size, size_t align);
} // namespace zarr