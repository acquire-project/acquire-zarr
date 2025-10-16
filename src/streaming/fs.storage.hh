#pragma once

#include "array.base.hh"
#include "file.handle.hh"

#include <memory>

namespace zarr {
class FSStorage
{
  public:
    explicit FSStorage(std::shared_ptr<FileHandlePool> file_handle_pool);
    virtual ~FSStorage() = default;

  protected:
    std::shared_ptr<FileHandlePool> file_handle_pool_;

    /**
     * @brief Write binary data to a path at the given offset.
     * @param path The path to write to.
     * @param data The data to write.
     * @param offset The offset to write at.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] bool write_binary_(const std::string& path,
                                     const std::vector<uint8_t>& data,
                                     size_t offset) const;

    /**
     * @brief Write a string to a path at the given offset.
     * @param path The path to write to.
     * @param data The string to write.
     * @param offset The offset to write at.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] bool write_string_(const std::string& path,
                                     const std::string& data,
                                     size_t offset) const;
};
} // namespace zarr