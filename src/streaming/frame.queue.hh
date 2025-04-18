#pragma once

#include "definitions.hh"

#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstddef>

namespace zarr {
class FrameQueue
{
  public:
    explicit FrameQueue(size_t num_frames, size_t avg_frame_size);
    ~FrameQueue() = default;

    bool push(ConstByteSpan frame);
    bool pop(ByteVector& frame);

    size_t size() const;
    size_t bytes_used() const;
    bool full() const;
    bool empty() const;

  private:
    struct Frame
    {
        ByteVector data;
        std::atomic<bool> ready{ false };
    };

    std::vector<Frame> buffer_;
    size_t capacity_;

    // Producer and consumer positions
    std::atomic<size_t> write_pos_{ 0 };
    std::atomic<size_t> read_pos_{ 0 };
};
} // namespace zarr