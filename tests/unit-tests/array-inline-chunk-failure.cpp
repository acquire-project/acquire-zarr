#include "array.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <filesystem>

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

const unsigned int array_width = 64, array_height = 48, array_frames = 4;
const unsigned int chunk_width = 16, chunk_height = 16, chunk_frames = 2;

std::unique_ptr<zarr::Array>
make_array(const fs::path& path)
{
    // A pool that no longer accepts jobs makes every chunk job run inline on
    // the calling thread, as it does during the last flush at close. The
    // unknown Blosc codec makes every chunk job fail.
    auto thread_pool = std::make_shared<zarr::ThreadPool>(
      2, [](const std::string& err) { LOG_ERROR("Error: ", err); });
    thread_pool->await_stop();

    std::vector<ZarrDimension> dims;
    dims.emplace_back(
      "t", ZarrDimensionType_Time, array_frames, chunk_frames, 1);
    dims.emplace_back(
      "y", ZarrDimensionType_Space, array_height, chunk_height, 1);
    dims.emplace_back("x", ZarrDimensionType_Space, array_width, chunk_width, 1);

    auto config = std::make_shared<zarr::ArrayConfig>(
      path.string(),
      "",
      std::nullopt,
      zarr::BloscCompressionParams("no-such-codec", 1, 1),
      std::make_shared<ArrayDimensions>(std::move(dims), ZarrDataType_uint8),
      ZarrDataType_uint8,
      std::nullopt,
      0,
      false);

    return std::make_unique<zarr::Array>(
      config, thread_pool, std::make_shared<zarr::FileHandlePool>(), nullptr);
}

bool
test_failure_during_streaming()
{
    const auto path = base_dir / "streaming";
    auto array = make_array(path);

    std::vector<uint8_t> frame(array_width * array_height, 1);
    size_t bytes_out = 0;
    CHECK(array->write_frame(frame, bytes_out, 0) == zarr::WriteResult::Ok);

    // the second frame completes a chunk layer and flushes it
    bool reported = false;
    try {
        reported =
          array->write_frame(frame, bytes_out, 1) != zarr::WriteResult::Ok;
    } catch (const std::exception& exc) {
        LOG_INFO("write_frame threw: ", exc.what());
        reported = true;
    }
    if (!reported) {
        LOG_ERROR("a failed inline chunk write was not reported");
    }
    return reported;
}

bool
test_failure_at_close()
{
    const auto path = base_dir / "close";
    auto array = make_array(path);

    // half a chunk layer: nothing is flushed until close
    std::vector<uint8_t> frame(array_width * array_height, 1);
    size_t bytes_out = 0;
    CHECK(array->write_frame(frame, bytes_out, 0) == zarr::WriteResult::Ok);

    const bool reported = !zarr::finalize_array(std::move(array));
    if (!reported) {
        LOG_ERROR("a failed inline chunk write at close was not reported");
    }
    return reported;
}
} // namespace

int
main()
{
    Logger::set_log_level(LogLevel_Debug);

    int retval = 1;

    try {
        const bool streaming_ok = test_failure_during_streaming();
        const bool close_ok = test_failure_at_close();
        retval = streaming_ok && close_ok ? 0 : 1;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    std::error_code ec;
    fs::remove_all(base_dir, ec);

    return retval;
}
