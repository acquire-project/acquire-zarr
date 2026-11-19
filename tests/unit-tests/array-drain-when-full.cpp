#include "array.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <filesystem>

namespace fs = std::filesystem;

/**
 * An array keeps its chunk and compression buffers for reuse between layers.
 * A bounded array frees them once it has its last frame, so a stream that fills
 * several arrays in turn does not keep a layer of buffers per finished array.
 * An unbounded array cannot know it is finished, so it keeps them until close.
 *
 * Chunk jobs run on the thread pool, so each case stops the pool before
 * measuring: await_stop() finishes every queued job, and a job's chunk is
 * destroyed before its worker takes the next, so every buffer is back in its
 * pool by then.
 */
namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

constexpr unsigned int n_frames = 4; // two layers of two frames
constexpr unsigned int chunk_t = 2, height = 64, width = 64, chunk_px = 32;
constexpr ZarrDataType dtype = ZarrDataType_uint16;
constexpr size_t frame_bytes = height * width * 2;

struct TestArray
{
    std::unique_ptr<zarr::Array> array;
    std::shared_ptr<zarr::ThreadPool> thread_pool;
};

TestArray
make_array(const fs::path& path, uint32_t t_size)
{
    std::vector<ZarrDimension> dims;
    dims.emplace_back("t", ZarrDimensionType_Time, t_size, chunk_t, 1);
    dims.emplace_back("y", ZarrDimensionType_Space, height, chunk_px, 1);
    dims.emplace_back("x", ZarrDimensionType_Space, width, chunk_px, 1);

    auto config = std::make_shared<zarr::ArrayConfig>(
      path.string(),
      "",
      std::nullopt,
      zarr::BloscCompressionParams("lz4", 1, 1),
      std::make_shared<ArrayDimensions>(std::move(dims), dtype),
      dtype,
      std::nullopt,
      0,
      false);

    auto thread_pool = std::make_shared<zarr::ThreadPool>(
      2, [](const std::string& err) { LOG_ERROR("Error: ", err); });

    auto array = std::make_unique<zarr::Array>(
      config, thread_pool, std::make_shared<zarr::FileHandlePool>(), nullptr);
    return { std::move(array), std::move(thread_pool) };
}

void
write_frames(zarr::Array& array)
{
    std::vector<uint8_t> frame(frame_bytes);
    for (auto i = 0u; i < n_frames; ++i) {
        std::fill(frame.begin(), frame.end(), uint8_t(1 + i));
        size_t bytes_out = 0;
        CHECK(array.write_frame(frame, bytes_out, i) == zarr::WriteResult::Ok);
        EXPECT_EQ(size_t, bytes_out, frame_bytes);
    }
}

void
test_bounded_array_drains()
{
    auto [array, thread_pool] = make_array(base_dir / "bounded", n_frames);

    // the last frame completes the array, so nothing is kept
    write_frames(*array);
    thread_pool->await_stop();
    EXPECT_EQ(size_t, array->memory_usage(), 0);

    CHECK(zarr::finalize_array(std::move(array)));
}

void
test_unbounded_array_keeps_buffers()
{
    auto [array, thread_pool] = make_array(base_dir / "unbounded", 0);

    // the same frames, but the array may take more, so it keeps one layer of
    // chunk buffers (2 x 2 tiles) for the next
    write_frames(*array);
    thread_pool->await_stop();
    const size_t chunk_bytes = chunk_t * chunk_px * chunk_px * 2;
    const size_t usage = array->memory_usage();
    EXPECT(usage >= 4 * chunk_bytes,
           "Expected at least one layer of kept buffers, got ",
           usage,
           " bytes");

    CHECK(zarr::finalize_array(std::move(array)));
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        test_bounded_array_drains();
        test_unbounded_array_keeps_buffers();
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    if (fs::exists(base_dir)) {
        fs::remove_all(base_dir);
    }

    return retval;
}
