#include "array.writer.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

namespace {
class TestWriter : public zarr::ArrayWriter
{
  public:
    TestWriter(zarr::ArrayWriterConfig&& array_spec,
               std::shared_ptr<zarr::ThreadPool> thread_pool)
      : zarr::ArrayWriter(std::move(array_spec), thread_pool)
    {
    }

  private:
    ZarrVersion version_() const override { return ZarrVersionCount; }
    bool should_rollover_() const override { return false; }
    bool compress_and_flush_data_() override { return true; }
    bool write_array_metadata_() override { return true; }
};
} // namespace

int
main()
{
    const auto base_dir = fs::temp_directory_path() / TEST;
    int retval = 1;

    const unsigned int array_width = 64, array_height = 48, array_planes = 2,
                       array_channels = 1, array_timepoints = 2;
    const unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 1,
                       chunk_channels = 1, chunk_timepoints = 1;

    const unsigned int n_frames =
      array_planes * array_channels * array_timepoints;

    const ZarrDataType dtype = ZarrDataType_uint16;
    const unsigned int nbytes_px = zarr::bytes_of_type(dtype);

    try {
        auto thread_pool = std::make_shared<zarr::ThreadPool>(
          std::thread::hardware_concurrency(),
          [](const std::string& err) { LOG_ERROR("Error: ", err); });

        std::vector<ZarrDimension> dims;
        dims.emplace_back(
          "t", ZarrDimensionType_Time, array_timepoints, chunk_timepoints, 0);
        dims.emplace_back(
          "c", ZarrDimensionType_Channel, array_channels, chunk_channels, 0);
        dims.emplace_back(
          "z", ZarrDimensionType_Space, array_planes, chunk_planes, 0);
        dims.emplace_back(
          "y", ZarrDimensionType_Space, array_height, chunk_height, 0);
        dims.emplace_back(
          "x", ZarrDimensionType_Space, array_width, chunk_width, 0);

        zarr::ArrayWriterConfig config = {
            .dimensions =
              std::make_unique<ArrayDimensions>(std::move(dims), dtype),
            .dtype = dtype,
            .bucket_name = std::nullopt,
            .store_path = base_dir.string(),
            .compression_params = std::nullopt,
        };

        TestWriter writer(std::move(config), thread_pool);

        const size_t frame_size = array_width * array_height * nbytes_px;
        std::vector data_(frame_size, std::byte(0));
        std::span data(data_.data(), data_.size());

        for (auto i = 0; i < n_frames; ++i) {
            CHECK(writer.write_frame(data) == frame_size);
        }

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    // cleanup
    if (fs::exists(base_dir)) {
        fs::remove_all(base_dir);
    }
    return retval;
}