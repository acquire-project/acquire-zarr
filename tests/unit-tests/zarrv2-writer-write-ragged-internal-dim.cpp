#include "zarrv2.array.writer.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>
#include <filesystem>

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

const unsigned int array_width = 64, array_height = 48, array_planes = 5,
                   array_timepoints = 5;
const unsigned int n_frames = array_planes * array_timepoints;

const unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2,
                   chunk_timepoints = 5;

const unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
const unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks
const unsigned int chunks_in_z =
  (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks, ragged
const unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints; // 1 chunk

const int level_of_detail = 2;
} // namespace

void
check_json()
{
    fs::path zarray_path =
      base_dir / std::to_string(level_of_detail) / ".zarray";
    CHECK(fs::is_regular_file(zarray_path));

    std::ifstream f(zarray_path);
    nlohmann::json zarray = nlohmann::json::parse(f);

    EXPECT(zarray["dtype"].get<std::string>() == "<f8",
           "Expected dtype to be <f8, but got ",
           zarray["dtype"].get<std::string>());

    EXPECT_EQ(int, zarray["zarr_format"].get<int>(), 2);

    const auto& chunks = zarray["chunks"];
    EXPECT_EQ(int, chunks.size(), 4);
    EXPECT_EQ(int, chunks[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, chunks[1].get<int>(), chunk_planes);
    EXPECT_EQ(int, chunks[2].get<int>(), chunk_height);
    EXPECT_EQ(int, chunks[3].get<int>(), chunk_width);

    const auto& shape = zarray["shape"];
    EXPECT_EQ(int, shape.size(), 4);
    EXPECT_EQ(int, shape[0].get<int>(), array_timepoints);
    EXPECT_EQ(int, shape[1].get<int>(), array_planes);
    EXPECT_EQ(int, shape[2].get<int>(), array_height);
    EXPECT_EQ(int, shape[3].get<int>(), array_width);
}

int
main()
{
    Logger::set_log_level(LogLevel_Debug);

    int retval = 1;

    const ZarrDataType dtype = ZarrDataType_float64;
    const unsigned int nbytes_px = zarr::bytes_of_type(dtype);

    try {
        auto thread_pool = std::make_shared<zarr::ThreadPool>(
          std::thread::hardware_concurrency(), [](const std::string& err) {
              LOG_ERROR("Error: ", err);
          });

        std::vector<ZarrDimension> dims;
        dims.emplace_back(
          "t", ZarrDimensionType_Time, array_timepoints, chunk_timepoints, 0);
        dims.emplace_back(
          "z", ZarrDimensionType_Space, array_planes, chunk_planes, 0);
        dims.emplace_back(
          "y", ZarrDimensionType_Space, array_height, chunk_height, 0);
        dims.emplace_back(
          "x", ZarrDimensionType_Space, array_width, chunk_width, 0);

        zarr::ArrayWriterConfig config = {
            .dimensions = std::make_shared<ArrayDimensions>(std::move(dims), dtype),
            .dtype = dtype,
            .level_of_detail = level_of_detail,
            .bucket_name = std::nullopt,
            .store_path = base_dir.string(),
            .compression_params = std::nullopt,
        };

        {
            auto writer = std::make_unique<zarr::ZarrV2ArrayWriter>(
              std::move(config), thread_pool);

            const size_t frame_size = array_width * array_height * nbytes_px;
            std::vector data(frame_size, std::byte(0));

            for (auto i = 0; i < n_frames; ++i) { // 2 time points
                CHECK(writer->write_frame(data));
            }

            CHECK(finalize_array(std::move(writer)));
        }

        check_json();

        const auto expected_file_size = chunk_width * chunk_height *
                                        chunk_planes * chunk_timepoints *
                                        nbytes_px;

        const fs::path data_root =
          base_dir / std::to_string(config.level_of_detail);

        CHECK(fs::is_directory(data_root));

        for (auto t = 0; t < chunks_in_t; ++t) {
            const auto t_dir = data_root / std::to_string(t);
            CHECK(fs::is_directory(t_dir));
            {
                for (auto z = 0; z < chunks_in_z; ++z) {
                    const auto z_dir = t_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < chunks_in_y; ++y) {
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < chunks_in_x; ++x) {
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            EXPECT_EQ(int, file_size, expected_file_size);
                        }

                        CHECK(!fs::is_regular_file(
                          y_dir / std::to_string(chunks_in_x)));
                    }

                    CHECK(
                      !fs::is_directory(z_dir / std::to_string(chunks_in_y)));
                }

                CHECK(
                  !fs::is_directory(t_dir / std::to_string(chunks_in_z)));
            }

            CHECK(!fs::is_directory(data_root / std::to_string(chunks_in_t)));
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