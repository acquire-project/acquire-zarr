#pragma once

#include "definitions.hh"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>

namespace zarr {
class FrameQueue
{
  public:
    explicit FrameQueue(size_t num_frames, size_t avg_frame_size);
    ~FrameQueue() = default;

    bool push(const std::span<const uint8_t>& frame,
              const std::string& key,
              uint64_t frame_id);

    bool pop(std::vector<uint8_t>& frame, std::string& key_, uint64_t& frame_id_);

    size_t size() const;
    size_t bytes_used() const;
    bool full() const;
    bool empty() const;
    void clear();

  private:
    struct Frame
    {
        std::string key;
        std::vector<uint8_t> data;
        uint64_t frame_id;
        std::atomic<bool> ready{ false };
    };

    std::vector<Frame> buffer_;
    size_t capacity_;

    // Producer and consumer positions
    std::atomic<size_t> write_pos_{ 0 };
    std::atomic<size_t> read_pos_{ 0 };

    mutable std::mutex mutex_;
};
} // namespace zarr