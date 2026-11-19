#include "unit.test.macros.hh"
#include "buffer.pool.hh"

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>

namespace {
constexpr size_t buffer_size = 4096;

bool
all_zero(const ByteVector& buffer)
{
    return std::all_of(
      buffer.begin(), buffer.end(), [](uint8_t b) { return b == 0; });
}
} // namespace

void
test_fresh_buffers_are_zeroed()
{
    zarr::BufferPool pool(buffer_size, 2);

    for (const bool zero : { true, false }) {
        const auto buffer = pool.acquire(zero);
        EXPECT_EQ(size_t, buffer.size(), buffer_size);
        CHECK(all_zero(buffer));
    }
    EXPECT_EQ(size_t, pool.retained_bytes(), 0);
}

void
test_reuse()
{
    zarr::BufferPool pool(buffer_size, 2);

    auto buffer = pool.acquire(true);
    std::fill(buffer.begin(), buffer.end(), 0xAB);
    const auto* data = buffer.data();
    pool.release(std::move(buffer));
    EXPECT_EQ(size_t, pool.retained_bytes(), buffer_size);

    // the same allocation comes back, zeroed on request
    auto zeroed = pool.acquire(true);
    CHECK(zeroed.data() == data);
    EXPECT_EQ(size_t, zeroed.size(), buffer_size);
    CHECK(all_zero(zeroed));
    EXPECT_EQ(size_t, pool.retained_bytes(), 0);

    // and kept as it was otherwise
    std::fill(zeroed.begin(), zeroed.end(), 0xCD);
    pool.release(std::move(zeroed));
    const auto kept = pool.acquire(false);
    CHECK(kept.data() == data);
    CHECK(std::all_of(
      kept.begin(), kept.end(), [](uint8_t b) { return b == 0xCD; }));
}

void
test_bound()
{
    constexpr size_t max_free = 3;
    zarr::BufferPool pool(buffer_size, max_free);

    std::vector<ByteVector> buffers;
    for (size_t i = 0; i < max_free + 2; ++i) {
        buffers.push_back(pool.acquire(true));
    }
    for (auto& buffer : buffers) {
        pool.release(std::move(buffer));
    }

    // the extra buffers were freed, not kept
    EXPECT_EQ(size_t, pool.retained_bytes(), max_free * buffer_size);
}

void
test_wrong_size_is_not_kept()
{
    zarr::BufferPool pool(buffer_size, 2);

    pool.release(ByteVector(buffer_size - 1, 1));
    pool.release(ByteVector());
    EXPECT_EQ(size_t, pool.retained_bytes(), 0);

    const auto buffer = pool.acquire(false);
    EXPECT_EQ(size_t, buffer.size(), buffer_size);
    CHECK(all_zero(buffer));
}

void
test_concurrent_use()
{
    constexpr size_t max_free = 4;
    constexpr int n_threads = 8, n_iterations = 1000;
    zarr::BufferPool pool(buffer_size, max_free);

    // an exception thrown on a std::thread terminates, so count failures
    std::atomic<int> failures{ 0 };
    std::vector<std::thread> threads;
    for (int t = 0; t < n_threads; ++t) {
        threads.emplace_back([&pool, &failures, t] {
            for (int i = 0; i < n_iterations; ++i) {
                auto buffer = pool.acquire(true);
                if (buffer.size() != buffer_size || !all_zero(buffer)) {
                    ++failures;
                }
                std::fill(buffer.begin(), buffer.end(), uint8_t(t + 1));
                pool.release(std::move(buffer));
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(int, failures.load(), 0);
    CHECK(pool.retained_bytes() <= max_free * buffer_size);
}

int
main()
{
    int retval = 1;

    try {
        test_fresh_buffers_are_zeroed();
        test_reuse();
        test_bound();
        test_wrong_size_is_not_kept();
        test_concurrent_use();
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR(e.what());
    }

    return retval;
}
