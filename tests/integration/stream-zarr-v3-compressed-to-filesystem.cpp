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

const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                   shard_channels = 2, shard_timepoints = 2;
const unsigned int chunks_per_shard =
  shard_width * shard_height * shard_planes * shard_channels * shard_timepoints;

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

const unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
const unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
const unsigned int shards_in_z =
  (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
const unsigned int shards_in_c =
  (chunks_in_c + shard_channels - 1) / shard_channels; // 1 shard
const unsigned int shards_in_t =
  (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

const size_t nbytes_px = sizeof(uint16_t);
const uint32_t frames_to_acquire =
  array_planes * array_channels * array_timepoints;
const size_t bytes_of_frame = array_width * array_height * nbytes_px;
} // namespace/s

ZarrStream*
setup()
{
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .data_type = ZarrDataType_uint16,
        .version = ZarrVersion_3,
        .max_threads = 0, // use all available threads
    };

    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 2,
        .shuffle = 2,
    };
    settings.compression_settings = &compression_settings;

    CHECK_OK(ZarrStreamSettings_create_dimension_array(&settings, 5));

    ZarrDimensionProperties* dim;
    dim = settings.dimensions;
    *dim = DIM("t", ZarrDimensionType_Time, array_timepoints, chunk_timepoints, shard_timepoints);

    dim = settings.dimensions + 1;
    *dim = DIM("c", ZarrDimensionType_Channel, array_channels, chunk_channels, shard_channels);

    dim = settings.dimensions + 2;
    *dim = DIM("z", ZarrDimensionType_Space, array_planes, chunk_planes, shard_planes);

    dim = settings.dimensions + 3;
    *dim = DIM("y", ZarrDimensionType_Space, array_height, chunk_height, shard_height);

    dim = settings.dimensions + 4;
    *dim = DIM("x", ZarrDimensionType_Space, array_width, chunk_width, shard_width);

    auto* stream = ZarrStream_create(&settings);
    ZarrStreamSettings_destroy_dimension_array(&settings);

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

    // multiscales metadata
    const auto multiscales = meta["attributes"]["multiscales"][0];

    const auto axes = multiscales["axes"];
    EXPECT_EQ(size_t, axes.size(), 5);
    std::string name, type, unit;

    name = axes[0]["name"];
    type = axes[0]["type"];
    EXPECT(name == "t", "Expected name to be 't', but got '", name, "'");
    EXPECT(type == "time", "Expected type to be 'time', but got '", type, "'");

    name = axes[1]["name"];
    type = axes[1]["type"];
    EXPECT(name == "c", "Expected name to be 'c', but got '", name, "'");
    EXPECT(
      type == "channel", "Expected type to be 'channel', but got '", type, "'");

    name = axes[2]["name"];
    type = axes[2]["type"];
    EXPECT(name == "z", "Expected name to be 'z', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");

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
    EXPECT(
      type == "scale", "Expected type to be 'scale', but got '", type, "'");

    const auto scale = coordinate_transformations["scale"];
    EXPECT_EQ(size_t, scale.size(), 5);
    EXPECT_EQ(int, scale[0].get<double>(), 1.0);
    EXPECT_EQ(int, scale[1].get<double>(), 1.0);
    EXPECT_EQ(int, scale[2].get<double>(), 1.0);
    EXPECT_EQ(int, scale[3].get<double>(), 1.0);
    EXPECT_EQ(int, scale[4].get<double>(), 1.0);
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

    const auto& chunks = meta["chunk_grid"]["configuration"]["chunk_shape"];
    EXPECT_EQ(size_t, chunks.size(), 5);
    EXPECT_EQ(int, chunks[0].get<int>(), chunk_timepoints* shard_timepoints);
    EXPECT_EQ(int, chunks[1].get<int>(), chunk_channels* shard_channels);
    EXPECT_EQ(int, chunks[2].get<int>(), chunk_planes* shard_planes);
    EXPECT_EQ(int, chunks[3].get<int>(), chunk_height* shard_height);
    EXPECT_EQ(int, chunks[4].get<int>(), chunk_width* shard_width);

    const auto dtype = meta["data_type"].get<std::string>();
    EXPECT(dtype == "uint16",
           "Expected dtype to be 'uint16', but got '",
           dtype,
           "'");

    const auto& codecs = meta["codecs"];
    EXPECT_EQ(size_t, codecs.size(), 1);
    const auto& sharding_codec = codecs[0]["configuration"];

    const auto& shards = sharding_codec["chunk_shape"];
    EXPECT_EQ(size_t, shards.size(), 5);
    EXPECT_EQ(int, shards[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, shards[1].get<int>(), chunk_channels);
    EXPECT_EQ(int, shards[2].get<int>(), chunk_planes);
    EXPECT_EQ(int, shards[3].get<int>(), chunk_height);
    EXPECT_EQ(int, shards[4].get<int>(), chunk_width);

    const auto& internal_codecs = sharding_codec["codecs"];
    EXPECT(internal_codecs.size() == 2,
           "Expected 2 internal codecs, got ",
           internal_codecs.size());

    EXPECT(internal_codecs[0]["name"].get<std::string>() == "bytes",
           "Expected first codec to be 'bytes', got ",
           internal_codecs[0]["name"].get<std::string>());
    EXPECT(internal_codecs[1]["name"].get<std::string>() == "blosc",
           "Expected second codec to be 'blosc', got ",
           internal_codecs[1]["name"].get<std::string>());

    const auto& blosc_codec = internal_codecs[1];
    const auto& blosc_config = blosc_codec["configuration"];
    EXPECT_EQ(int, blosc_config["blocksize"].get<int>(), 0);
    EXPECT_EQ(int, blosc_config["clevel"].get<int>(), 2);
    EXPECT(blosc_config["cname"].get<std::string>() == "lz4",
           "Expected codec name to be 'lz4', got ",
           blosc_config["cname"].get<std::string>());
    EXPECT(blosc_config["shuffle"].get<std::string>() == "bitshuffle",
           "Expected shuffle to be 'bitshuffle', got ",
           blosc_config["shuffle"].get<std::string>());
    EXPECT_EQ(int, blosc_config["typesize"].get<int>(), 2);
}

void
verify_file_data()
{
    const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                            chunk_channels * chunk_timepoints * nbytes_px;
    const auto index_size = chunks_per_shard *
                            sizeof(uint64_t) * // indices are 64 bits
                            2;                 // 2 indices per chunk
    const auto checksum_size = 4;              // crc32 checksum is 4 bytes
    const auto expected_file_size = shard_width * shard_height * shard_planes *
                                      shard_channels * shard_timepoints *
                                      chunk_size +
                                    index_size + checksum_size;

    fs::path data_root = fs::path(test_path) / "0";

    CHECK(fs::is_directory(data_root));
    for (auto t = 0; t < shards_in_t; ++t) {
        const auto t_dir = data_root / "c" / std::to_string(t);
        CHECK(fs::is_directory(t_dir));

        for (auto c = 0; c < shards_in_c; ++c) {
            const auto c_dir = t_dir / std::to_string(c);
            CHECK(fs::is_directory(c_dir));

            for (auto z = 0; z < shards_in_z; ++z) {
                const auto z_dir = c_dir / std::to_string(z);
                CHECK(fs::is_directory(z_dir));

                for (auto y = 0; y < shards_in_y; ++y) {
                    const auto y_dir = z_dir / std::to_string(y);
                    CHECK(fs::is_directory(y_dir));

                    for (auto x = 0; x < shards_in_x; ++x) {
                        const auto x_file = y_dir / std::to_string(x);
                        CHECK(fs::is_regular_file(x_file));
                        const auto file_size = fs::file_size(x_file);
                        EXPECT_LT(size_t, file_size, expected_file_size);
                    }

                    CHECK(!fs::is_regular_file(y_dir /
                                               std::to_string(shards_in_x)));
                }

                CHECK(!fs::is_directory(z_dir / std::to_string(shards_in_y)));
            }

            CHECK(!fs::is_directory(c_dir / std::to_string(shards_in_z)));
        }

        CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_c)));
    }

    CHECK(!fs::is_directory(data_root / "c" / std::to_string(shards_in_t)));
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
        std::ifstream f = std::ifstream(group_metadata_path);
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
    Zarr_set_log_level(ZarrLogLevel_Debug);

    auto* stream = setup();
    std::vector<uint16_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out);
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
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

    return retval;
}
