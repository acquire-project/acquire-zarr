#pragma once

#include "s3.object.hh"

#include <unordered_map>

namespace zarr {
class S3Storage
{
  public:
    S3Storage(const std::string& bucket_name,
              std::shared_ptr<S3ConnectionPool> s3_connection_pool);
    virtual ~S3Storage() = default;

    /**
     * @brief Write binary data to a path at the given offset.
     * @param key The path to write to.
     * @param data The data to write.
     * @param offset The offset to write at.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] bool write_binary(const std::string& key,
                                    const std::vector<uint8_t>& data,
                                    size_t offset);

    /**
     * @brief Write a string to a path at the given offset.
     * @param key The path to write to.
     * @param data The string to write.
     * @param offset The offset to write at.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] bool write_string(const std::string& key,
                                    const std::string& data,
                                    size_t offset);

    /**
     * @brief Finalize the object at the given path.
     * @details This will ensure that any buffered data is flushed and the
     * object is properly closed.
     * @param path The path of the object to finalize.
     * @return True if the object was successfully finalized, otherwise false.
     */
    [[nodiscard]] bool finalize_object(const std::string& path);

  protected:
    const std::string bucket_name_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    void create_s3_object_(const std::string& key);

    std::unordered_map<std::string, std::unique_ptr<S3Object>> s3_objects_;
};
} // namespace zarr