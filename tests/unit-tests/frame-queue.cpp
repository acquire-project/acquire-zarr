#include "unit.test.macros.hh"
#include "frame.queue.hh"

#include <chrono>
#include <thread>
#include <iostream>
#include <vector>

void
test_basic_operations()
{
    zarr::FrameQueue queue(10, 1024);

    // Initial state
    CHECK(queue.size() == 0);
    CHECK(queue.empty());
    CHECK(!queue.full());

    ByteVector frame(1024);
    for (size_t i = 0; i < frame.size(); ++i) {
        frame[i] = std::byte(i % 256);
    }

    // Pushing
    CHECK(queue.push(frame));
    CHECK(queue.size() == 1);
    CHECK(!queue.empty());

    // Popping
    ByteVector received_frame;
    CHECK(queue.pop(received_frame));
    CHECK(received_frame.size() == 1024);
    CHECK(queue.size() == 0);
    CHECK(queue.empty());

    // Verify data
    for (size_t i = 0; i < received_frame.size(); ++i) {
        CHECK(received_frame[i] == std::byte(i % 256));
    }
}

void
test_capacity()
{
    const size_t capacity = 5;
    zarr::FrameQueue queue(capacity, 100);

    // Fill the queue
    for (size_t i = 0; i < capacity; ++i) {
        ByteVector frame(100, std::byte(i));
        bool result = queue.push(frame);
        CHECK(result);
    }

    // Queue should be full (next push should fail)
    ByteVector extra_frame(100);
    bool push_result = queue.push(extra_frame);
    CHECK(!push_result);
    CHECK(queue.size() == capacity);

    // Remove one item
    ByteVector received_frame;
    bool pop_result = queue.pop(received_frame);
    CHECK(pop_result);
    CHECK(queue.size() == capacity - 1);

    // Should be able to push again
    ByteVector new_frame(100, std::byte(99));
    push_result = queue.push(new_frame);
    CHECK(push_result);
    CHECK(queue.size() == capacity);
}

// Test producer-consumer pattern with threads
void
test_producer_consumer()
{
    const size_t n_frames = 1000;
    const size_t frame_size = 1024;
    const size_t queue_capacity = 10;

    zarr::FrameQueue queue(queue_capacity, frame_size);

    // Producer thread
    std::thread producer([&queue, n_frames, frame_size]() {
        for (size_t i = 0; i < n_frames; ++i) {
            ByteVector frame(frame_size, std::byte(i % 256));

            // Try until successful
            while (!queue.push(frame)) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    });

    // Consumer thread
    std::thread consumer([&queue, n_frames]() {
        size_t frames_received = 0;

        while (frames_received < n_frames) {
            ByteVector frame;
            if (queue.pop(frame)) {
                // Verify frame data (first byte should match frame number %
                // 256)
                CHECK(frame.size() > 0);
                CHECK(std::to_integer<int>(frame[0]) ==
                      (frames_received % 256));
                frames_received++;
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    });

    producer.join();
    consumer.join();

    CHECK(queue.empty());
}

// Test high throughput
void
test_throughput()
{
    // Create a queue that can hold 2 seconds of data at 2 GiB/s
    const auto buffer_size = 4ULL << 30;      // 4 GiB
    const auto frame_size = 16 * 1024 * 1024; // 16 MiB frames
    const auto num_frames = buffer_size / frame_size;

    zarr::FrameQueue queue(num_frames, frame_size);

    // Create large frame for testing
    std::vector large_frame(frame_size, std::byte(42));

    auto start_time = std::chrono::high_resolution_clock::now();

    // Push and pop in a loop
    const size_t iterations = 100;
    ByteVector received_frame;
    for (size_t i = 0; i < iterations; ++i) {
        CHECK(queue.push(ByteVector(large_frame)));

        CHECK(queue.pop(received_frame));
        CHECK(received_frame.size() == frame_size);

        received_frame.clear();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

    double throughput = (static_cast<double>(iterations) * frame_size * 2) /
                        (duration.count() / 1000.0) / (1024 * 1024 * 1024);

    LOG_INFO("Throughput test: ", throughput, " GiB/s");
}

int
main()
{
    int retval = 1;

    try {
        test_basic_operations();
        test_capacity();
        test_producer_consumer();
        test_throughput();
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR(e.what());
    }

    return retval;
}