#include "buffer.pool.hh"
#include "macros.hh"

#include <algorithm>

zarr::BufferPool::BufferPool(size_t buffer_size, size_t max_free)
  : buffer_size_(buffer_size)
  , max_free_(max_free)
{
    EXPECT(buffer_size_ > 0, "Empty buffer size");
    free_.reserve(max_free_);
}

ByteVector
zarr::BufferPool::acquire(bool zero)
{
    ByteVector buffer;
    {
        std::unique_lock lock(mutex_);
        if (!free_.empty()) {
            buffer = std::move(free_.back());
            free_.pop_back();
        }
    }

    if (buffer.empty()) {
        return ByteVector(buffer_size_, 0);
    }

    // a reused buffer's pages are already resident, so this is a plain memset
    // rather than the page faults and kernel zeroing of a fresh allocation
    if (zero) {
        std::fill(buffer.begin(), buffer.end(), 0);
    }

    return buffer;
}

void
zarr::BufferPool::release(ByteVector buffer)
{
    if (buffer.size() != buffer_size_) {
        return;
    }

    std::unique_lock lock(mutex_);
    if (free_.size() < max_free_) {
        free_.push_back(std::move(buffer));
    }
    // otherwise buffer is freed on return, after the lock is released
}

void
zarr::BufferPool::drain()
{
    std::vector<ByteVector> to_free;
    {
        std::unique_lock lock(mutex_);
        max_free_ = 0;
        to_free.swap(free_);
    }
    // to_free is destroyed here, outside the lock
}

size_t
zarr::BufferPool::buffer_size() const noexcept
{
    return buffer_size_;
}

size_t
zarr::BufferPool::retained_bytes() const noexcept
{
    std::unique_lock lock(mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        return 0;
    }

    return free_.size() * buffer_size_;
}
