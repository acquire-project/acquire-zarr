#include "frame.queue.hh"
#include "macros.hh"

#include <cstring>
#include <stdexcept>

zarr::FrameQueue::FrameQueue(size_t num_frames, size_t avg_frame_size)
  : buffer_(num_frames + 1)   // one extra slot to distinguish full/empty
  , capacity_(num_frames + 1) // one extra slot to distinguish full/empty
{
    EXPECT(num_frames > 0, "FrameQueue must have at least one frame.");

    for (auto& frame : buffer_) {
        frame.ready.store(false, std::memory_order_relaxed);
    }

    write_pos_.store(0, std::memory_order_relaxed);
    read_pos_.store(0, std::memory_order_relaxed);
}

bool
zarr::FrameQueue::push(LockedBuffer& frame, const std::string& key)
{
    std::unique_lock lock(mutex_);
    size_t write_pos = write_pos_.load(std::memory_order_relaxed);

    size_t next_pos = (write_pos + 1) % capacity_;
    if (next_pos == read_pos_.load(std::memory_order_acquire)) {
        return false; // Queue is full
    }

    buffer_[write_pos].key = key;
    buffer_[write_pos].data.swap(frame);
    buffer_[write_pos].ready.store(true, std::memory_order_release);

    write_pos_.store(next_pos, std::memory_order_release);

    return true;
}

bool
zarr::FrameQueue::pop(LockedBuffer& frame, std::string& key)
{
    std::unique_lock lock(mutex_);
    size_t read_pos = read_pos_.load(std::memory_order_relaxed);

    if (read_pos == write_pos_.load(std::memory_order_acquire)) {
        return false; // Queue is empty
    }

    if (!buffer_[read_pos].ready.load(std::memory_order_acquire)) {
        return false;
    }

    key = buffer_[read_pos].key;
    frame.swap(buffer_[read_pos].data);
    buffer_[read_pos].ready.store(false, std::memory_order_release);

    read_pos_.store((read_pos + 1) % capacity_, std::memory_order_release);

    return true;
}

size_t
zarr::FrameQueue::size() const
{
    auto write = write_pos_.load(std::memory_order_relaxed);
    auto read = read_pos_.load(std::memory_order_relaxed);

    if (write >= read) {
        return write - read;
    } else {
        return capacity_ - (read - write);
    }
}

size_t
zarr::FrameQueue::bytes_used() const
{
    size_t total_bytes = 0;

    size_t write = write_pos_.load(std::memory_order_relaxed);
    size_t read = read_pos_.load(std::memory_order_relaxed);

    // Iterate through occupied slots
    size_t pos = read;
    while (pos != write) {
        if (buffer_[pos].ready.load(std::memory_order_relaxed)) {
            total_bytes += buffer_[pos].data.size();
        }
        pos = (pos + 1) % capacity_;
    }

    return total_bytes;
}

bool
zarr::FrameQueue::full() const
{
    // Queue is full when the next write position equals read position
    size_t write = write_pos_.load(std::memory_order_relaxed);
    size_t next_write = (write + 1) % capacity_;
    size_t read = read_pos_.load(std::memory_order_acquire);

    return (next_write == read);
}

bool
zarr::FrameQueue::empty() const
{
    // Queue is empty when read position equals write position
    // and the slot at read position is not ready
    size_t read = read_pos_.load(std::memory_order_relaxed);
    size_t write = write_pos_.load(std::memory_order_acquire);

    return (read == write);
}

void
zarr::FrameQueue::clear()
{
    std::unique_lock lock(mutex_);
    read_pos_.store(write_pos_.load(std::memory_order_acquire),
                    std::memory_order_release);
}