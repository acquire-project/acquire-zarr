#pragma once

#include <cstddef> // size_t
#include <string>

/**
 * @brief Get the system page size.
 * @throw std::runtime_error if the page size cannot be determined.
 * @return The system page size in bytes.
 */
size_t
get_page_size();

/**
 * @brief Get the sector size for the volume containing the given path.
 * @param path A path on the volume to query.
 * @throw std::runtime_error if the sector size cannot be determined.
 * @return The sector size in bytes.
 */
size_t
get_system_alignment_size(const std::string& path);

/**
 * @brief Align a size to the system page size.
 * @param size The size to align.
 * @param align The system size to align to (e.g., page size or sector size).
 * @return The aligned size.
 */
size_t
align_to_system_size(size_t size, size_t align);

/**
 * @brief Get the last error message as a string.
 * @return The last error message, or an empty string if there is no error.
 */
std::string
get_last_error_as_string();