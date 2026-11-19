#include "array.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

// one frame is one layer of 2 x 2 chunks of 32 x 32 uint16
const unsigned int frame_px = 64, chunk_px = 32, n_frames = 50;
const size_t chunk_bytes = chunk_px * chunk_px * sizeof(uint16_t);
const size_t layer_bytes = 4 * chunk_bytes;
} // namespace

int
main()
{
    if (std::thread::hardware_concurrency() < 2) {
        LOG_INFO("Skipping: a one-core pool runs chunk jobs inline");
        return 0;
    }

    int retval = 1;
    std::promise<void> gate;
    std::shared_future<void> open = gate.get_future().share();
    bool gate_open = false;

    // outside the try block, so that a failed check still joins the writer
    // before the array it writes to is destroyed
    std::unique_ptr<zarr::Array> array;
    std::atomic<unsigned int> written{ 0 };
    std::atomic<bool> failed{ false };
    std::thread writer;

    try {
        fs::remove_all(base_dir);

        auto thread_pool = std::make_shared<zarr::ThreadPool>(
          3, [](const std::string& err) { LOG_ERROR("Error: ", err); });
        const size_t n_threads = thread_pool->n_threads();
        const size_t workers = std::max<size_t>(n_threads, 2) - 1;
        const size_t budget =
          chunk_bytes * std::max<size_t>(2 * 4, 4 * workers);

        // a compressor that never finishes: every pool thread waits on the gate
        for (size_t i = 0; i < n_threads; ++i) {
            CHECK(thread_pool->push_job([open](std::string&) {
                open.wait();
                return zarr::ThreadPool::TaskResult::Success;
            }));
        }

        std::vector<ZarrDimension> dims;
        dims.emplace_back("t", ZarrDimensionType_Time, 0, 1, 1);
        dims.emplace_back("y", ZarrDimensionType_Space, frame_px, chunk_px, 2);
        dims.emplace_back("x", ZarrDimensionType_Space, frame_px, chunk_px, 2);

        auto config = std::make_shared<zarr::ArrayConfig>(
          base_dir.string(),
          "",
          std::nullopt,
          std::nullopt,
          std::make_shared<ArrayDimensions>(std::move(dims),
                                            ZarrDataType_uint16),
          ZarrDataType_uint16,
          std::nullopt,
          0,
          false);

        array = std::make_unique<zarr::Array>(
          config,
          thread_pool,
          std::make_shared<zarr::FileHandlePool>(),
          nullptr);

        writer = std::thread([&] {
            std::vector<uint8_t> frame(frame_px * frame_px * sizeof(uint16_t),
                                       1);
            for (unsigned int i = 0; i < n_frames; ++i) {
                size_t bytes_out = 0;
                if (array->write_frame(frame, bytes_out, i) !=
                    zarr::WriteResult::Ok) {
                    failed = true;
                    return;
                }
                ++written;
            }
        });

        // the writer stalls with the budget in jobs and its next layer in the
        // slots: wait for exactly that, up to 10 s
        const size_t stalled = layer_bytes + budget;
        size_t usage = 0;
        for (int t = 0; t < 1000 && written.load() < n_frames; ++t) {
            usage = array->memory_usage();
            if (usage == stalled) {
                break;
            }
            std::this_thread::sleep_for(10ms);
        }

        EXPECT(
          written.load() < n_frames,
          "Expected the writer to block with no worker free; it wrote all ",
          n_frames,
          " frames");
        EXPECT(usage == stalled,
               "Expected memory_usage() to count the queued chunks: ",
               stalled,
               " bytes; got ",
               usage);
        // hold for a moment: the writer must stay blocked
        std::this_thread::sleep_for(200ms);
        EXPECT_EQ(size_t, array->memory_usage(), stalled);

        gate.set_value();
        gate_open = true;
        writer.join();
        CHECK(!failed);
        EXPECT_EQ(unsigned int, written.load(), n_frames);
        CHECK(zarr::finalize_array(std::move(array)));

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    if (!gate_open) {
        gate.set_value(); // let the pool threads and the writer finish
    }
    if (writer.joinable()) {
        writer.join();
    }
    array.reset();
    fs::remove_all(base_dir);
    return retval;
}
