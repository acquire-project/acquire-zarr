#include "zarrv2.array.writer.hh"
#include "unit.test.macros.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

#include <filesystem>

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = fs::temp_directory_path() / TEST;

const unsigned int array_width = 64, array_height = 48, array_planes = 6,
                   array_channels = 8, array_timepoints = 10;
const unsigned int n_frames = array_planes * array_channels * array_timepoints;

const unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2,
                   chunk_channels = 4, chunk_timepoints = 5;

const unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
const unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks

const unsigned int chunks_in_z =
  (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks
const unsigned int chunks_in_c =
  (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
const unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints; // 2 chunks

const int level_of_detail = 0;
} // namespace

void
check_json()
{
    fs::path meta_path =
      base_dir / std::to_string(level_of_detail) / ".zarray";
    CHECK(fs::is_regular_file(meta_path));

    std::ifstream f(meta_path);
    nlohmann::json meta = nlohmann::json::parse(f);

    EXPECT(meta["dtype"].get<std::string>() == "<u2",
           "Expected dtype to be <u2, but got ",
           meta["dtype"].get<std::string>());

    EXPECT_EQ(int, meta["zarr_format"].get<int>(), 2);

    const auto& array_shape = meta["shape"];
    const auto& chunk_shape = meta["chunks"];

    EXPECT_EQ(int, array_shape.size(), 5);
    EXPECT_EQ(int, array_shape[0].get<int>(), array_timepoints);
    EXPECT_EQ(int, array_shape[1].get<int>(), array_channels);
    EXPECT_EQ(int, array_shape[2].get<int>(), array_planes);
    EXPECT_EQ(int, array_shape[3].get<int>(), array_height);
    EXPECT_EQ(int, array_shape[4].get<int>(), array_width);

    EXPECT_EQ(int, chunk_shape.size(), 5);
    EXPECT_EQ(int, chunk_shape[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, chunk_shape[1].get<int>(), chunk_channels);
    EXPECT_EQ(int, chunk_shape[2].get<int>(), chunk_planes);
    EXPECT_EQ(int, chunk_shape[3].get<int>(), chunk_height);
    EXPECT_EQ(int, chunk_shape[4].get<int>(), chunk_width);
}

void
print_frame(const ByteVector& frame)
{
    for (auto i = 0; i < array_height; ++i) {
        for (auto j = 0; j < array_width; ++j) {
            std::cout << (int)frame[i * array_width + j] << " ";
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

void
fill_frame(ByteVector& frame,
           std::shared_ptr<ArrayDimensions> dims,
           int frame_id)
{
    // fill each tile in the frame with a value for each chunk
    const auto t = dims->chunk_lattice_index(frame_id, 0);
    const auto c = dims->chunk_lattice_index(frame_id, 1);
    const auto z = dims->chunk_lattice_index(frame_id, 2);

    const auto base_fill_value = t * 10000 + c * 1000 + z * 100;

    int k = 0;
    for (auto i = 0; i < array_height; ++i) {
        const auto y = i / chunk_height;
        for (auto j = 0; j < array_width; ++j) {
            const auto x = j / chunk_width;
            const auto fill_value = base_fill_value + y * 10 + x;
            frame[k++] = static_cast<std::byte>(fill_value);
        }
    }

//    print_frame(frame);
}

void
validate_chunk_data(const ByteVector& chunk_data,
                    std::vector<uint32_t> lattice_idx)
{
    const auto t = lattice_idx[0];
    const auto c = lattice_idx[1];
    const auto z = lattice_idx[2];
    const auto y = lattice_idx[3];
    const auto x = lattice_idx[4];

    const auto fill_value = t * 10000 + c * 1000 + z * 100 + y * 10 + x;

    int k = 0;
    for (auto i = 0; i < chunk_height; ++i) {
        for (auto j = 0; j < chunk_width; ++j) {
            EXPECT(chunk_data[k++] == static_cast<std::byte>(fill_value),
                   "Expected fill value ",
                   fill_value,
                   " at (",
                   i,
                   ", ",
                   j,
                   ") in chunk (",
                   t,
                   ",",
                   c,
                   ",",
                   z,
                   ",",
                   y,
                   ",",
                   x,
                   ") but got ",
                   (int)chunk_data[k - 1]);
        }
    }
}

int
main()
{
    Logger::set_log_level(LogLevel_Debug);

    int retval = 1;

    const ZarrDataType dtype = ZarrDataType_uint16;
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
          "c", ZarrDimensionType_Channel, array_channels, chunk_channels, 0);
        dims.emplace_back(
          "z", ZarrDimensionType_Space, array_planes, chunk_planes, 0);
        dims.emplace_back(
          "y", ZarrDimensionType_Space, array_height, chunk_height, 0);
        dims.emplace_back(
          "x", ZarrDimensionType_Space, array_width, chunk_width, 0);

        const auto array_dims =
          std::make_shared<ArrayDimensions>(std::move(dims), dtype);

        zarr::ArrayWriterConfig config = {
            .dimensions = array_dims,
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
                fill_frame(data, array_dims, i);
                CHECK(writer->write_frame(data));
            }

            CHECK(finalize_array(std::move(writer)));
        }

        check_json();

        const auto expected_file_size = chunk_width * chunk_height *
                                        chunk_planes * chunk_channels *
                                        chunk_timepoints * nbytes_px;

        const fs::path data_root =
          base_dir / std::to_string(config.level_of_detail);

        std::vector<uint32_t> lattice_idx(5, 0);

        CHECK(fs::is_directory(data_root));
        for (auto t = 0; t < chunks_in_t; ++t) {
            lattice_idx[0] = t;
            const auto t_dir = data_root / std::to_string(t);
            CHECK(fs::is_directory(t_dir));

            for (auto c = 0; c < chunks_in_c; ++c) {
                lattice_idx[1] = c;
                const auto c_dir = t_dir / std::to_string(c);
                CHECK(fs::is_directory(c_dir));

                for (auto z = 0; z < chunks_in_z; ++z) {
                    lattice_idx[2] = z;
                    const auto z_dir = c_dir / std::to_string(z);
                    CHECK(fs::is_directory(z_dir));

                    for (auto y = 0; y < chunks_in_y; ++y) {
                        lattice_idx[3] = y;
                        const auto y_dir = z_dir / std::to_string(y);
                        CHECK(fs::is_directory(y_dir));

                        for (auto x = 0; x < chunks_in_x; ++x) {
                            lattice_idx[4] = x;
                            const auto x_file = y_dir / std::to_string(x);
                            CHECK(fs::is_regular_file(x_file));
                            const auto file_size = fs::file_size(x_file);
                            EXPECT_EQ(int, file_size, expected_file_size);

                            ByteVector chunk_data(file_size);
                            std::ifstream f(x_file, std::ios::binary);
                            f.read(reinterpret_cast<char*>(chunk_data.data()),
                                   file_size);
                            CHECK(f.gcount() == file_size);
                            validate_chunk_data(chunk_data, lattice_idx);
                        }

                        CHECK(!fs::is_regular_file(
                          y_dir / std::to_string(chunks_in_x)));
                    }

                    CHECK(
                      !fs::is_directory(z_dir / std::to_string(chunks_in_y)));
                }

                CHECK(!fs::is_directory(c_dir / std::to_string(chunks_in_z)));
            }

            CHECK(!fs::is_directory(t_dir / std::to_string(chunks_in_c)));
        }

        CHECK(!fs::is_directory(data_root / std::to_string(chunks_in_t)));

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