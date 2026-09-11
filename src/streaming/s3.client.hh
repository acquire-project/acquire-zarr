#pragma once

#include "definitions.hh"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace zarr {
struct S3Settings
{
    std::string endpoint;
    std::string bucket_name;
    std::optional<std::string> region;
};

/**
 * @brief Where an object's requests are sent.
 */
struct S3RequestTarget
{
    std::string uri;  ///< Scheme and authority, without a trailing slash.
    std::string path; ///< Request path, with a leading slash.
};

/**
 * @brief Resolve the request target for one object.
 * @details Virtual-host addressing is used for `*.amazonaws.com` endpoints,
 * which require it, and path style otherwise, which most other S3-compatible
 * servers require. A bucket whose name is not a legal DNS label -- because it
 * contains a dot, an underscore or an upper-case letter -- falls back to path
 * style even on AWS, since it cannot be prepended to the host. S3Client calls
 * this for every request; it is declared here so the addressing rules can be
 * tested without a server.
 * @param endpoint Endpoint URL, which must begin with http:// or https://. The
 * scheme and host are matched case-insensitively, and any path, query or
 * fragment is discarded.
 * @param bucket_name The name of the bucket.
 * @param object_name The key within the bucket. Empty addresses the bucket
 * itself.
 * @returns The endpoint URI and request path to use.
 * @throws std::runtime_error if the endpoint has no scheme or no host.
 */
S3RequestTarget s3_request_target(const std::string& endpoint,
                                  std::string_view bucket_name,
                                  std::string_view object_name);

/**
 * @brief Check that an endpoint URL can be used for S3 requests.
 * @details Applies exactly the rules s3_request_target() applies, so settings
 * validation and the client cannot disagree about what is acceptable.
 * @param endpoint Endpoint URL to check.
 * @param error Set to the reason the endpoint was rejected, untouched
 * otherwise.
 * @returns True if the endpoint has a supported scheme and a host.
 */
[[nodiscard]] bool
is_valid_s3_endpoint(const std::string& endpoint, std::string& error);

/**
 * @brief A single append-only upload of one S3 object.
 * @details Wraps one CRT PutObject meta request in async-write mode: the CRT
 * splits the byte stream into parts, uploads them in parallel, retries failed
 * parts, and completes the multipart upload, so the object size need not be
 * known up front.
 * @note Bytes must be appended in order, and only one append may be in flight
 * at a time. This class will not serialize for you; see S3Sink, which reorders
 * the concurrent out-of-order writes it receives before appending here.
 */
class S3Upload
{
  public:
    ~S3Upload();

    /**
     * @brief Append @p data to the end of the object.
     * @details Blocks until the CRT is ready to accept more data.
     * @param data The bytes to append. May be any size.
     * @returns True if the data was accepted, otherwise false.
     */
    [[nodiscard]] bool append(ConstByteSpan data);

    /**
     * @brief Signal end of data and wait for the object to be stored.
     * @details Idempotent: subsequent calls return the first call's result.
     * @returns True if and only if the object was stored successfully.
     */
    [[nodiscard]] bool finish();

    /**
     * @brief Cancel the upload, discarding any parts already uploaded.
     */
    void abort();

  private:
    friend class S3Client;

    struct Impl;
    std::unique_ptr<Impl> impl_;

    explicit S3Upload(std::unique_ptr<Impl> impl);
};

/**
 * @brief A thread-safe S3 client for one endpoint and region.
 * @details Backed by a single CRT S3 client, which multiplexes any number of
 * concurrent requests over connection and thread pools it owns. One instance is
 * shared by every writer thread; unlike the connection pool this replaces,
 * there is nothing to check out or return.
 *
 * Bucket addressing is chosen from the endpoint and the bucket name:
 * virtual-host style for `*.amazonaws.com` with a bucket that is a legal DNS
 * label, path style otherwise. See s3_request_target().
 *
 * Credentials come from the CRT's default chain (environment, AWS config
 * profile, ECS, then IMDS) and are never taken from the public API.
 */
class S3Client
{
  public:
    /**
     * @brief Construct a client for the endpoint in @p settings.
     * @param settings Endpoint, bucket and optional region. The endpoint must
     * begin with http:// or https://. An absent region is signed as
     * "us-east-1"; endpoints that validate the signing region require it to be
     * set explicitly to match the server.
     * @throws std::runtime_error if the endpoint has no scheme or no host, or
     * if the underlying CRT client cannot be created.
     */
    explicit S3Client(const S3Settings& settings);
    ~S3Client();

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
     * @returns True if the object exists, otherwise false. An empty bucket or
     * object name returns false rather than raising.
     */
    bool object_exists(std::string_view bucket_name,
                       std::string_view object_name);

    /**
     * @brief Put an object in a single request.
     * @details For objects small enough to be held in memory. Larger objects,
     * and objects of unknown size, should use create_upload().
     * @param bucket_name The name of the bucket to put the object in.
     * @param object_name The name of the object.
     * @param data The data to put in the object.
     * @returns True if the object was stored, otherwise false.
     * @throws std::runtime_error if the bucket name is empty, the object name
     * is empty, or @p data is empty.
     */
    [[nodiscard]] bool put_object(std::string_view bucket_name,
                                  std::string_view object_name,
                                  ConstByteSpan data);

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

    /**
     * @brief Get an object's size in bytes.
     * @details Provided, like object_exists() and delete_object(), so callers
     * and tests can verify what was written; the streaming path does not use it.
     * @param bucket_name The name of the bucket containing the object.
     * @param object_name The name of the object.
     * @returns The size in bytes, or nullopt if the object could not be reached.
     * @throws std::runtime_error if the bucket name or object name is empty.
     */
    [[nodiscard]] std::optional<size_t> object_size(
      std::string_view bucket_name,
      std::string_view object_name);

    /**
     * @brief Fetch an object's contents.
     * @param bucket_name The name of the bucket containing the object.
     * @param object_name The name of the object.
     * @returns The object's bytes, or nullopt if it could not be fetched.
     * @throws std::runtime_error if the bucket name or object name is empty.
     */
    [[nodiscard]] std::optional<ByteVector> get_object(
      std::string_view bucket_name,
      std::string_view object_name);

    /**
     * @brief Begin a streaming upload of an object of unknown size.
     * @param bucket_name The name of the bucket to put the object in.
     * @param object_name The name of the object.
     * @returns The upload, or nullptr if it could not be started.
     * @throws std::runtime_error if the bucket name or object name is empty.
     */
    [[nodiscard]] std::unique_ptr<S3Upload> create_upload(
      std::string_view bucket_name,
      std::string_view object_name);

  private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};
} // namespace zarr
