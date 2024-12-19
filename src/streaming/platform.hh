#pragma once

#include <span>
#include <string_view>
#include <vector>

namespace zarr {
class VectorizedFile
{
  public:
    explicit VectorizedFile(std::string_view path);
    ~VectorizedFile();

  private:
    void* inner_;

    friend bool file_write_vectorized(
      VectorizedFile& file,
      const std::vector<std::span<std::byte>>& buffers,
      size_t offset);
};

/**
 * @brief Align and offset or a size to the nearest system boundary.
 * @note Aligns to sector size on Windows, page size on UNIX.
 * @param size The offset or size to align.
 * @return The aligned offset or size.
 */
size_t
align_to_system_boundary(size_t size);

/**
 * @brief Write a vector of buffers to the file at the given path.
 * @param[in] file The VectorizedFile to write to.
 * @param[in] buffers The buffers to write.
 * @param[in] offset The offset in the file to write to. This value must be
 * aligned to the system boundary.
 * @throws std::runtime_error if the file cannot be opened for writing, or if
 * the offset is not aligned to the system boundary.
 * @return True if the write was successful, false otherwise.
 */
bool
file_write_vectorized(VectorizedFile& file,
                      const std::vector<std::span<std::byte>>& buffers,
                      size_t offset);
}