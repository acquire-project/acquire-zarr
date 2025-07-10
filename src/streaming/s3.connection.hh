#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <span>
#include <vector>

namespace zarr {
struct S3Settings
{
    std::string endpoint;
    std::string bucket_name;
    std::optional<std::string> region;
};

struct S3Part
{
    unsigned int number;
    std::string etag;
    size_t size;
};

class S3Connection
{
  public:
    explicit S3Connection(const S3Settings& settings);
    ~S3Connection();

    /* Bucket operations */

    /**
     * @brief Check whether a bucket exists.
     * @param bucket_name The name of the bucket.
     * @returns True if the bucket exists, otherwise false.
     */
    bool bucket_exists(std::string_view bucket_name);

    /* Object operations */

    /**
     * @brief Check whether an object exists.
     * @param bucket_name The name of the bucket containing the object.
     * @param object_name The name of the object.
     * @returns True if the object exists, otherwise false.
     */
    bool object_exists(std::string_view bucket_name,
                       std::string_view object_name);

    /**
     * @brief Put an object.
     * @param bucket_name The name of the bucket to put the object in.
     * @param object_name The name of the object.
     * @param data The data to put in the object.
     * @returns The etag of the object.
     * @throws std::runtime_error if the bucket name is empty, the object name
     * is empty, or @p data is empty.
     */
    [[nodiscard]] std::string put_object(std::string_view bucket_name,
                                         std::string_view object_name,
                                         std::span<uint8_t> data);

    /**
     * @brief Delete an object.
     * @param bucket_name The name of the bucket containing the object.
     * @param object_name The name of the object.
     * @returns True if the object was successfully deleted, otherwise false.
     * @throws std::runtime_error if the bucket name is empty or the object
     * name is empty.
     */
    [[nodiscard]] bool delete_object(std::string_view bucket_name,
                                     std::string_view object_name);

    /* Multipart object operations */

    /// @brief Create a multipart object.
    /// @param bucket_name The name of the bucket containing the object.
    /// @param object_name The name of the object.
    /// @returns The upload id of the multipart object. Nonempty if and only if
    ///          the operation succeeds.
    /// @throws std::runtime_error if the bucket name is empty or the object
    ///         name is empty.
    [[nodiscard]] std::string create_multipart_object(
      std::string_view bucket_name,
      std::string_view object_name);

    /// @brief Upload a part of a multipart object.
    /// @param bucket_name The name of the bucket containing the object.
    /// @param object_name The name of the object.
    /// @param upload_id The upload id of the multipart object.
    /// @param data The data to upload.
    /// @param part_number The part number of the object.
    /// @returns The etag of the uploaded part. Nonempty if and only if the
    ///          operation is successful.
    /// @throws std::runtime_error if the bucket name is empty, the object name
    ///         is empty, @p data is empty, or @p part_number is 0.
    [[nodiscard]] std::string upload_multipart_object_part(
      std::string_view bucket_name,
      std::string_view object_name,
      std::string_view upload_id,
      std::span<uint8_t> data,
      unsigned int part_number);

    /// @brief Complete a multipart object.
    /// @param bucket_name The name of the bucket containing the object.
    /// @param object_name The name of the object.
    /// @param upload_id The upload id of the multipart object.
    /// @param parts List of the parts making up the object.
    /// @returns True if the object was successfully completed, otherwise false.
    [[nodiscard]] bool complete_multipart_object(
      std::string_view bucket_name,
      std::string_view object_name,
      std::string_view upload_id,
      const std::vector<S3Part>& parts);

  private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

class S3ConnectionPool
{
  public:
    S3ConnectionPool(size_t n_connections, const S3Settings& settings);
    ~S3ConnectionPool();

    std::unique_ptr<S3Connection> get_connection();
    void return_connection(std::unique_ptr<S3Connection>&& conn);

  private:
    std::vector<std::unique_ptr<S3Connection>> connections_;
    std::mutex connections_mutex_;
    std::condition_variable cv_;

    std::atomic<bool> is_accepting_connections_{ true };
};
} // namespace zarr
