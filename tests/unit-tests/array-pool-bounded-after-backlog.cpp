#include "array.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;

/**
 * A banded array flushes one dim-1 band at a time, so it stays within about two
 * bands of chunk buffers. Chunk writes are queued with no backpressure, so a
 * slow sink can let every band of a layer queue up; when they finally write,
 * the array must keep no more than one band of their buffers for reuse, not
 * the whole layer.
 *
 * The pool's workers are held while a full layer is written, so every band's
 * flush queues behind them. Then they are released, and await_stop() finishes
 * the queue; a job's chunk is destroyed before its worker takes the next, so
 * every buffer is back in its pool by then.
 */
namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

// t is the append dimension with chunk 1, so the array bands along z: 16 bands
// of one 16-plane chunk each
constexpr unsigned int planes = 256, chunk_planes = 16, height = 32, width = 32;
constexpr ZarrDataType dtype = ZarrDataType_uint16;
constexpr size_t frame_bytes = height * width * 2;
constexpr size_t band_bytes = chunk_planes * frame_bytes; // one chunk

constexpr unsigned int n_workers = 2;
} // namespace

int
main()
{
    int retval = 1;

    try {
        std::vector<ZarrDimension> dims;
        dims.emplace_back("t", ZarrDimensionType_Time, 0, 1, 1);
        dims.emplace_back(
          "z", ZarrDimensionType_Space, planes, chunk_planes, 1);
        dims.emplace_back("y", ZarrDimensionType_Space, height, height, 1);
        dims.emplace_back("x", ZarrDimensionType_Space, width, width, 1);

        auto config = std::make_shared<zarr::ArrayConfig>(
          base_dir.string(),
          "",
          std::nullopt,
          std::nullopt,
          std::make_shared<ArrayDimensions>(std::move(dims), dtype),
          dtype,
          std::nullopt,
          0,
          false);
        CHECK(config->dimensions->supports_dim1_banding());
        EXPECT_EQ(size_t, config->dimensions->bytes_per_chunk(), band_bytes);

        auto thread_pool = std::make_shared<zarr::ThreadPool>(
          n_workers, [](const std::string& err) { LOG_ERROR("Error: ", err); });
        CHECK(thread_pool->n_threads() == n_workers);

        auto array = std::make_unique<zarr::Array>(
          config,
          thread_pool,
          std::make_shared<zarr::FileHandlePool>(),
          nullptr);

        // hold every worker until the whole layer has been written
        std::atomic<bool> release{ false };
        std::atomic<unsigned int> holding{ 0 };
        for (auto i = 0u; i < n_workers; ++i) {
            CHECK(thread_pool->push_job([&](std::string&) {
                ++holding;
                while (!release.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                return zarr::ThreadPool::TaskResult::Success;
            }));
        }
        while (holding.load() < n_workers) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        std::vector<uint8_t> frame(frame_bytes);
        for (auto z = 0u; z < planes; ++z) {
            std::fill(frame.begin(), frame.end(), uint8_t(1 + z % 250));
            size_t bytes_out = 0;
            CHECK(array->write_frame(frame, bytes_out, z) ==
                  zarr::WriteResult::Ok);
        }

        release = true;
        thread_pool->await_stop();

        // every band has been written; at most one band is kept for reuse
        const size_t usage = array->memory_usage();
        EXPECT(usage <= band_bytes,
               "Expected at most one band (",
               band_bytes,
               " bytes) kept after the backlog drained, got ",
               usage,
               " bytes");

        CHECK(zarr::finalize_array(std::move(array)));
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    if (fs::exists(base_dir)) {
        fs::remove_all(base_dir);
    }

    return retval;
}
