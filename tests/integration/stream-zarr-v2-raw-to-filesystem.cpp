#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <fstream>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();

const unsigned int array_width = 64, array_height = 48, array_planes = 6,
                   array_channels = 8, array_timepoints = 10;

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
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

const size_t nbytes_px = sizeof(int32_t);
const uint32_t frames_to_acquire =
  array_planes * array_channels * array_timepoints;
const size_t bytes_of_frame = array_width * array_height * nbytes_px;
} // namespace

ZarrStream*
setup()
{
    ZarrArraySettings array = {
        .compression_settings = nullptr,
        .data_type = ZarrDataType_int32,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .version = ZarrVersion_2,
        .max_threads = 0, // use all available threads
        .arrays = &array,
        .array_count = 1
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 5));

    ZarrDimensionProperties* dim;
    dim = settings.arrays->dimensions;
    *dim = DIM("t",
               ZarrDimensionType_Time,
               array_timepoints,
               chunk_timepoints,
               0,
               nullptr,
               1.0);

    dim = settings.arrays->dimensions + 1;
    *dim = DIM("c",
               ZarrDimensionType_Channel,
               array_channels,
               chunk_channels,
               0,
               nullptr,
               1.0);

    dim = settings.arrays->dimensions + 2;
    *dim = DIM("z",
               ZarrDimensionType_Space,
               array_planes,
               chunk_planes,
               0,
               "millimeter",
               1.4);

    dim = settings.arrays->dimensions + 3;
    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               chunk_height,
               0,
               "micrometer",
               0.9);

    dim = settings.arrays->dimensions + 4;
    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               chunk_width,
               0,
               "micrometer",
               0.9);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);

    return stream;
}

void
verify_base_metadata(const nlohmann::json& meta)
{
    const auto multiscales = meta["multiscales"][0];
    const auto ngff_version = multiscales["version"].get<std::string>();
    EXPECT(ngff_version == "0.4",
           "Expected version to be '0.4', but got '",
           ngff_version,
           "'");

    const auto axes = multiscales["axes"];
    EXPECT_EQ(size_t, axes.size(), 5);
    std::string name, type, unit;

    name = axes[0]["name"];
    type = axes[0]["type"];
    EXPECT(name == "t", "Expected name to be 't', but got '", name, "'");
    EXPECT(type == "time", "Expected type to be 'time', but got '", type, "'");
    EXPECT(!axes[0].contains("unit"),
           "Expected unit to be missing, got ",
           axes[0]["unit"].get<std::string>());

    name = axes[1]["name"];
    type = axes[1]["type"];
    EXPECT(name == "c", "Expected name to be 'c', but got '", name, "'");
    EXPECT(
      type == "channel", "Expected type to be 'channel', but got '", type, "'");
    EXPECT(!axes[1].contains("unit"),
           "Expected unit to be missing, got ",
           axes[1]["unit"].get<std::string>());

    name = axes[2]["name"];
    type = axes[2]["type"];
    unit = axes[2]["unit"];
    EXPECT(name == "z", "Expected name to be 'z', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "millimeter",
           "Expected unit to be 'millimeter', but got '",
           unit,
           "'");

    name = axes[3]["name"];
    type = axes[3]["type"];
    unit = axes[3]["unit"];
    EXPECT(name == "y", "Expected name to be 'y', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '",
           unit,
           "'");

    name = axes[4]["name"];
    type = axes[4]["type"];
    unit = axes[4]["unit"];
    EXPECT(name == "x", "Expected name to be 'x', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '",
           unit,
           "'");

    const auto datasets = multiscales["datasets"][0];
    const std::string path = datasets["path"].get<std::string>();
    EXPECT(path == "0", "Expected path to be '0', but got '", path, "'");

    const auto coordinate_transformations =
      datasets["coordinateTransformations"][0];

    type = coordinate_transformations["type"].get<std::string>();
    EXPECT(type == "scale", "Expected type to be 'scale', but got '", type, "'");

    const auto scale = coordinate_transformations["scale"];
    EXPECT_EQ(size_t, scale.size(), 5);
    EXPECT_EQ(double, scale[0].get<double>(), 1.0);
    EXPECT_EQ(double, scale[1].get<double>(), 1.0);
    EXPECT_EQ(double, scale[2].get<double>(), 1.4);
    EXPECT_EQ(double, scale[3].get<double>(), 0.9);
    EXPECT_EQ(double, scale[4].get<double>(), 0.9);
}

void
verify_group_metadata(const nlohmann::json& meta)
{
    const auto zarr_format = meta["zarr_format"].get<int>();
    EXPECT_EQ(int, zarr_format, 2);
}

void
verify_array_metadata(const nlohmann::json& meta)
{
    const auto& shape = meta["shape"];
    EXPECT_EQ(size_t, shape.size(), 5);
    EXPECT_EQ(int, shape[0].get<int>(), array_timepoints);
    EXPECT_EQ(int, shape[1].get<int>(), array_channels);
    EXPECT_EQ(int, shape[2].get<int>(), array_planes);
    EXPECT_EQ(int, shape[3].get<int>(), array_height);
    EXPECT_EQ(int, shape[4].get<int>(), array_width);

    const auto& chunks = meta["chunks"];
    EXPECT_EQ(size_t, chunks.size(), 5);
    EXPECT_EQ(int, chunks[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, chunks[1].get<int>(), chunk_channels);
    EXPECT_EQ(int, chunks[2].get<int>(), chunk_planes);
    EXPECT_EQ(int, chunks[3].get<int>(), chunk_height);
    EXPECT_EQ(int, chunks[4].get<int>(), chunk_width);

    const auto dtype = meta["dtype"].get<std::string>();
    EXPECT(dtype == "<i4",
           "Expected dtype to be '<i4', but got '%s'",
           dtype.c_str());

    const auto& compressor = meta["compressor"];
    EXPECT(compressor.is_null(),
           "Expected compressor to be null, but got '%s'",
           compressor.dump().c_str());
}

void
verify_file_data()
{
    const auto expected_file_size = chunk_width * chunk_height * chunk_planes *
                                    chunk_channels * chunk_timepoints *
                                    nbytes_px;

    fs::path data_root = fs::path(test_path) / "0";

    CHECK(fs::is_directory(data_root));
    for (auto t = 0; t < chunks_in_t; ++t) {
        const auto t_dir = data_root / std::to_string(t);
        CHECK(fs::is_directory(t_dir));

        for (auto c = 0; c < chunks_in_c; ++c) {
            const auto c_dir = t_dir / std::to_string(c);
            CHECK(fs::is_directory(c_dir));

            for (auto z = 0; z < chunks_in_z; ++z) {
                const auto z_dir = c_dir / std::to_string(z);
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < chunks_in_y; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < chunks_in_x; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        EXPECT(file_size == expected_file_size,
                               "Expected file size == ",
                               expected_file_size,
                               " for file ",
                               x_file.string(),
                               ", got ",
                               file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir /
                                               std::to_string(chunks_in_x)));
                }

                CHECK(!fs::is_directory(z_dir / std::to_string(chunks_in_y)));
            }

            CHECK(!fs::is_directory(c_dir / std::to_string(chunks_in_z)));
        }

        CHECK(!fs::is_directory(t_dir / std::to_string(chunks_in_c)));
    }

    CHECK(!fs::is_directory(data_root / std::to_string(chunks_in_t)));
}

void
verify()
{
    CHECK(std::filesystem::is_directory(test_path));

    {
        fs::path base_metadata_path = fs::path(test_path) / ".zattrs";
        std::ifstream f(base_metadata_path);
        nlohmann::json base_metadata = nlohmann::json::parse(f);

        verify_base_metadata(base_metadata);
    }

    {
        fs::path group_metadata_path = fs::path(test_path) / ".zgroup";
        std::ifstream f = std::ifstream(group_metadata_path);
        nlohmann::json group_metadata = nlohmann::json::parse(f);

        verify_group_metadata(group_metadata);
    }

    {
        fs::path array_metadata_path = fs::path(test_path) / "0" / ".zarray";
        std::ifstream f = std::ifstream(array_metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(f);

        verify_array_metadata(array_metadata);
    }

    verify_file_data();
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    auto* stream = setup();
    std::vector<int32_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ", i, ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame);
        }

        ZarrStream_destroy(stream);

        verify();

        // Clean up
        fs::remove_all(test_path);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    // cleanup
    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    return retval;
}
