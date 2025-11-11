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

constexpr unsigned int array_width = 2048, array_height = 2048,
                       array_planes = 1024;
constexpr unsigned int chunk_width = 64, chunk_height = 64, chunk_planes = 64;
constexpr unsigned int shard_width = 16, shard_height = 16, shard_planes = 1;
constexpr unsigned int chunks_per_shard =
  shard_width * shard_height * shard_planes;

constexpr unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
constexpr unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks
constexpr unsigned int chunks_in_z =
  (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks

constexpr unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
constexpr unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
constexpr unsigned int shards_in_z =
  (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards

constexpr size_t nbytes_px = sizeof(uint16_t);
constexpr uint32_t frames_to_acquire = array_planes;
constexpr size_t bytes_of_frame = array_width * array_height * nbytes_px;
} // namespace

ZarrStream*
setup()
{
    ZarrArraySettings array = {
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint16,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .max_threads = 0, // use all available threads
        .arrays = &array,
        .array_count = 1,
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 3));

    ZarrDimensionProperties* dim = settings.arrays->dimensions;
    *dim = DIM("z",
               ZarrDimensionType_Space,
               array_planes,
               chunk_planes,
               shard_planes,
               "millimeter",
               1.4);
    ++dim;

    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               chunk_height,
               shard_height,
               "micrometer",
               0.9);
    ++dim;

    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               chunk_width,
               shard_width,
               "micrometer",
               0.9);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);

    return stream;
}

void
verify_group_metadata(const nlohmann::json& meta)
{
    auto zarr_format = meta["zarr_format"].get<int>();
    EXPECT_EQ(int, zarr_format, 3);

    auto node_type = meta["node_type"].get<std::string>();
    EXPECT_STR_EQ(node_type.c_str(), "group");

    EXPECT(meta["consolidated_metadata"].is_null(),
           "Expected consolidated_metadata to be null");

    // OME metadata
    const auto ome = meta["attributes"]["ome"];
    const auto multiscales = ome["multiscales"][0];
    const auto ngff_version = ome["version"].get<std::string>();
    EXPECT(ngff_version == "0.5",
           "Expected version to be '0.5', but got '",
           ngff_version,
           "'");

    const auto axes = multiscales["axes"];
    EXPECT_EQ(size_t, axes.size(), 3);
    std::string name, type, unit;

    name = axes[0]["name"];
    type = axes[0]["type"];
    unit = axes[0]["unit"];
    EXPECT(name == "z", "Expected name to be 'z', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "millimeter",
           "Expected unit to be 'millimeter', but got '",
           unit,
           "'");

    name = axes[1]["name"];
    type = axes[1]["type"];
    unit = axes[1]["unit"];
    EXPECT(name == "y", "Expected name to be 'y', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '",
           unit,
           "'");

    name = axes[2]["name"];
    type = axes[2]["type"];
    unit = axes[2]["unit"];
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
    EXPECT(
      type == "scale", "Expected type to be 'scale', but got '", type, "'");

    const auto scale = coordinate_transformations["scale"];
    EXPECT_EQ(size_t, scale.size(), 3);
    EXPECT_EQ(int, scale[0].get<double>(), 1.4);
    EXPECT_EQ(int, scale[1].get<double>(), 0.9);
    EXPECT_EQ(int, scale[2].get<double>(), 0.9);
}

void
verify_array_metadata(const nlohmann::json& meta)
{
    const auto& shape = meta["shape"];
    EXPECT_EQ(size_t, shape.size(), 3);
    EXPECT_EQ(int, shape[0].get<int>(), array_planes);
    EXPECT_EQ(int, shape[1].get<int>(), array_height);
    EXPECT_EQ(int, shape[2].get<int>(), array_width);

    const auto& chunks = meta["chunk_grid"]["configuration"]["chunk_shape"];
    EXPECT_EQ(size_t, chunks.size(), 3);
    EXPECT_EQ(int, chunks[0].get<int>(), chunk_planes* shard_planes);
    EXPECT_EQ(int, chunks[1].get<int>(), chunk_height* shard_height);
    EXPECT_EQ(int, chunks[2].get<int>(), chunk_width* shard_width);

    const auto dtype = meta["data_type"].get<std::string>();
    EXPECT(dtype == "uint16",
           "Expected dtype to be 'uint16', but got '",
           dtype,
           "'");

    const auto& codecs = meta["codecs"];
    EXPECT_EQ(size_t, codecs.size(), 1);
    const auto& sharding_codec = codecs[0]["configuration"];

    const auto& shards = sharding_codec["chunk_shape"];
    EXPECT_EQ(size_t, shards.size(), 3);
    EXPECT_EQ(int, shards[0].get<int>(), chunk_planes);
    EXPECT_EQ(int, shards[1].get<int>(), chunk_height);
    EXPECT_EQ(int, shards[2].get<int>(), chunk_width);

    const auto& internal_codecs = sharding_codec["codecs"];
    EXPECT(internal_codecs.size() == 1,
           "Expected 1 internal codec, got ",
           internal_codecs.size());

    EXPECT(internal_codecs[0]["name"].get<std::string>() == "bytes",
           "Expected first codec to be 'bytes', got ",
           internal_codecs[0]["name"].get<std::string>());

    const auto& dimension_names = meta["dimension_names"];
    EXPECT_EQ(size_t, dimension_names.size(), 3);

    EXPECT(dimension_names[0].get<std::string>() == "z",
           "Expected third dimension name to be 'z', got ",
           dimension_names[0].get<std::string>());
    EXPECT(dimension_names[1].get<std::string>() == "y",
           "Expected fourth dimension name to be 'y', got ",
           dimension_names[1].get<std::string>());
    EXPECT(dimension_names[2].get<std::string>() == "x",
           "Expected fifth dimension name to be 'x', got ",
           dimension_names[2].get<std::string>());
}

void
verify_file_data()
{
    const auto chunk_size =
      chunk_width * chunk_height * chunk_planes * nbytes_px;
    const auto index_size = chunks_per_shard *
                            sizeof(uint64_t) * // indices are 64 bits
                            2;                 // 2 indices per chunk
    const auto checksum_size = 4;              // crc32 checksum is 4 bytes
    const auto expected_file_size =
      shard_width * shard_height * shard_planes * chunk_size + index_size +
      checksum_size;

    fs::path data_root = fs::path(test_path) / "0";

    CHECK(fs::is_directory(data_root));
    for (auto z = 0; z < shards_in_z; ++z) {
        const auto z_dir = data_root / "c" / std::to_string(z);
        CHECK(fs::is_directory(z_dir));

        for (auto y = 0; y < shards_in_y; ++y) {
            const auto y_dir = z_dir / std::to_string(y);
            CHECK(fs::is_directory(y_dir));

            for (auto x = 0; x < shards_in_x; ++x) {
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

            CHECK(!fs::is_regular_file(y_dir / std::to_string(shards_in_x)));
        }

        CHECK(!fs::is_directory(z_dir / std::to_string(shards_in_y)));
    }

    CHECK(!fs::is_directory(data_root / "c" / std::to_string(shards_in_z)));
}

void
verify()
{
    CHECK(std::filesystem::is_directory(test_path));

    {
        fs::path group_metadata_path = fs::path(test_path) / "zarr.json";
        EXPECT(fs::is_regular_file(group_metadata_path),
               "Expected file '",
               group_metadata_path,
               "' to exist");
        std::ifstream f(group_metadata_path);
        nlohmann::json group_metadata = nlohmann::json::parse(f);

        verify_group_metadata(group_metadata);
    }

    {
        fs::path array_metadata_path = fs::path(test_path) / "0" / "zarr.json";
        EXPECT(fs::is_regular_file(array_metadata_path),
               "Expected file '",
               array_metadata_path,
               "' to exist");
        std::ifstream f = std::ifstream(array_metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(f);

        verify_array_metadata(array_metadata);
    }

    verify_file_data();
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Info);

    auto* stream = setup();
    const std::vector<uint16_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            const auto t1 = std::chrono::high_resolution_clock::now();
            const ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
            const auto t2 = std::chrono::high_resolution_clock::now();
            const std::chrono::duration<double, std::milli> fp_ms = t2 - t1;
            LOG_INFO("Appending frame ", i, " took ", fp_ms.count(), " ms");
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame);
        }

        const auto t1 = std::chrono::high_resolution_clock::now();
        ZarrStream_destroy(stream);
        const auto t2 = std::chrono::high_resolution_clock::now();
        const std::chrono::duration<double, std::milli> fp_ms = t2 - t1;
        LOG_INFO("Closing stream took ", fp_ms.count(), " ms");

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
