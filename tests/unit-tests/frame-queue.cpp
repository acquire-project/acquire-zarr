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

    std::vector<uint8_t> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = i % 256;
    }
    std::vector<uint8_t> frame(std::move(data));

    // Pushing
    CHECK(queue.push(frame, "foo"));
    CHECK(queue.size() == 1);
    CHECK(!queue.empty());

    // Popping
    std::vector<uint8_t> received_frame;
    std::string received_key;
    CHECK(queue.pop(received_frame, received_key));
    CHECK(received_frame.size() == 1024);
    CHECK(queue.size() == 0);
    CHECK(queue.empty());

    // Verify data
    {
        for (size_t i = 0; i < received_frame.size(); ++i) {
            CHECK(received_frame[i] == i % 256);
        }
    }
    CHECK(received_key == "foo");
}

void
test_capacity()
{
    const size_t capacity = 5;
    zarr::FrameQueue queue(capacity, 100);

    // Fill the queue
    for (size_t i = 0; i < capacity; ++i) {
        std::vector<uint8_t> frame(std::move(std::vector<uint8_t>(100, i)));
        bool result = queue.push(frame, std::to_string(i));
        CHECK(result);
    }

    // Queue should be full (next push should fail)
    std::vector<uint8_t> extra_frame(std::move(std::vector<uint8_t>(100)));
    bool push_result = queue.push(extra_frame, std::to_string(capacity));
    CHECK(!push_result);
    CHECK(queue.size() == capacity);

    // Remove one item
    std::vector<uint8_t> received_frame;
    std::string received_key;
    bool pop_result = queue.pop(received_frame, received_key);
    CHECK(pop_result);
    CHECK(queue.size() == capacity - 1);
    CHECK(received_key == "0");

    // Should be able to push again
    std::vector<uint8_t> new_frame(std::move(std::vector<uint8_t>(100, 99)));
    push_result = queue.push(new_frame, std::to_string(capacity));
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
            std::vector<uint8_t> frame(
              std::move(std::vector<uint8_t>(frame_size, i % 256)));

            // Try until successful
            while (!queue.push(frame, "spam")) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    });

    // Consumer thread
    std::thread consumer([&queue, n_frames]() {
        size_t frames_received = 0;

        while (frames_received < n_frames) {
            std::vector<uint8_t> frame;
            std::string received_key;
            if (queue.pop(frame, received_key)) {
                // Verify frame data (first byte should match frame number %
                // 256)
                CHECK(frame.size() > 0);
                CHECK(frame[0] == frames_received % 256);
                CHECK(received_key == "spam");
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
    std::vector<uint8_t> large_frame(frame_size, 42);
    std::vector<uint8_t> data(std::move(std::vector<uint8_t>(large_frame)));

    auto start_time = std::chrono::high_resolution_clock::now();

    // Push and pop in a loop
    const size_t iterations = 100;
    std::vector<uint8_t> received_frame;
    std::string received_key;
    for (size_t i = 0; i < iterations; ++i) {
        CHECK(queue.push(data, std::to_string(i)));
        CHECK(queue.pop(received_frame, received_key));
        CHECK(received_frame.size() == frame_size);
        CHECK(received_key == std::to_string(i));
        data.resize(frame_size);
        std::ranges::fill(data, 42); // reset data for next push
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