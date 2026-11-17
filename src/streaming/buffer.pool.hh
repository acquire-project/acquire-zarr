#pragma once

#include "definitions.hh"

#include <cstddef>
#include <mutex>
#include <vector>

namespace zarr {
/**
 * @brief A bounded free list of equally sized byte buffers.
 * @details A chunk buffer is written once, compressed, written to its shard and
 * then discarded, so allocating a fresh one per chunk faults in new pages at
 * the stream's data rate and serializes on the kernel's page allocator.
 * Returning buffers here instead lets the next layer reuse their pages. The
 * pool keeps at most `max_free` buffers; any more are freed on release, so
 * it never holds more than its owner had live together. drain() frees them
 * once the owner will need no more.
 */
class BufferPool
{
  public:
    BufferPool(size_t buffer_size, size_t max_free);

    /**
     * @brief Take a buffer of exactly buffer_size() bytes.
     * @param zero If true, every byte of the buffer is zero, as for a freshly
     * value-initialized vector. If false, a reused buffer keeps whatever its
     * last user left in it.
     */
    ByteVector acquire(bool zero);

    /**
     * @brief Return a buffer for reuse. A buffer of the wrong size, or one
     * beyond the pool's bound, is freed instead.
     */
    void release(ByteVector buffer);

    /**
     * @brief Free every kept buffer, and free rather than keep any buffer
     * released from now on. For an owner that will acquire no more.
     */
    void drain();

    size_t buffer_size() const noexcept;

    /**
     * @brief Bytes held by the free list.
     * @details Best effort: reports 0 rather than waiting if the pool is busy,
     * since this only feeds a usage estimate. The pool is busy only for a
     * push or pop, but up to `max_free` buffers can go unreported for that
     * moment.
     */
    size_t retained_bytes() const noexcept;

  private:
    const size_t buffer_size_;
    size_t max_free_; // guarded by mutex_; 0 once drained

    mutable std::mutex mutex_;
    std::vector<ByteVector> free_;
};
} // namespace zarr
