#pragma once

#include "sink.hh"
#include "s3.client.hh"

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace zarr {
/**
 * @brief A Sink that streams one object to S3.
 * @details Writers hand this sink byte ranges of a shard concurrently and out of
 * order (Shard::write_chunk claims each chunk's offset under a lock but writes
 * outside it, and every chunk is its own thread-pool job). The underlying upload
 * is strictly append-only, so this sink stages fragments until they are
 * contiguous, then appends them in order, with exactly one writer thread
 * appending at a time.
 *
 * Objects small enough never to spill are sent as a single request; anything
 * larger streams, letting the CRT split, parallelize and retry parts.
 */
class S3Sink : public Sink
{
  public:
    S3Sink(std::string_view bucket_name,
           std::string_view object_key,
           std::shared_ptr<S3Client> client);

    bool write(size_t offset, ConstByteSpan data) override;

    /**
     * @brief Bytes staged for upload but not yet handed to it.
     * @details Out-of-order writers can leave most of a shard sitting in
     * pending_ while the fragment that unblocks them is still being written,
     * so this is worth reporting rather than assuming it away. Bytes already
     * handed to an in-flight append are not counted: that is one batch at
     * most, and it is released whether the append returns or throws.
     * @return The number of bytes staged.
     */
    size_t memory_usage() const noexcept override;

  protected:
    bool flush_() override;

  private:
    /// Bytes to accumulate before streaming rather than sending one request.
    /// Matches S3's minimum upload part size.
    static constexpr size_t min_part_size_ = 5 << 20;

    std::string bucket_name_;
    std::string object_key_;
    std::shared_ptr<S3Client> client_;

    std::mutex mutex_;
    std::condition_variable cv_;

    /// Fragments that arrived before the bytes preceding them, keyed by offset.
    std::map<size_t, ByteVector> pending_;

    /// Contiguous bytes not yet handed to the upload.
    ByteVector staged_;

    /// Bytes already handed to the upload.
    size_t nbytes_appended_{ 0 };

    /// pending_ and staged_ sizes, readable without the lock.
    std::atomic<size_t> staging_bytes_{ 0 };

    /// Whether a thread is currently appending. Only one may, so the others
    /// stage their bytes and let the appending thread pick them up. flush_
    /// holds this for the whole of finalization.
    bool draining_{ false };

    /// Whether finalization has begun. Writes after that point cannot reach
    /// the object, so they are refused rather than silently staged.
    bool closing_{ false };

    bool failed_{ false };

    std::unique_ptr<S3Upload> upload_;

    /**
     * @brief Move any now-contiguous fragments from pending_ into staged_.
     * @note Caller must hold mutex_.
     */
    void coalesce_();

    /**
     * @brief Append staged bytes to the upload, opening it if needed.
     * @details Releases @p lock while appending, so other writers can stage.
     * @return True on success, otherwise false.
     */
    [[nodiscard]] bool drain_(std::unique_lock<std::mutex>& lock);
};
} // namespace zarr
