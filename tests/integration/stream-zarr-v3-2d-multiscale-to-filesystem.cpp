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

const unsigned int array_width = 64, array_height = 48, array_channels = 8,
                   array_timepoints = 10;

const unsigned int chunk_width = 16, chunk_height = 16, chunk_channels = 4,
                   chunk_timepoints = 5;

const unsigned int shard_width = 2, shard_height = 1, shard_channels = 2,
                   shard_timepoints = 2;
const unsigned int chunks_per_shard =
  shard_width * shard_height * shard_channels * shard_timepoints;

const unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
const unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks
const unsigned int chunks_in_c =
  (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
const unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

const unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
const unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
const unsigned int shards_in_c =
  (chunks_in_c + shard_channels - 1) / shard_channels; // 1 shard
const unsigned int shards_in_t =
  (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

const size_t nbytes_px = sizeof(uint16_t);
const uint32_t frames_to_acquire = array_channels * array_timepoints;
const size_t bytes_of_frame = array_width * array_height * nbytes_px;
} // namespace

ZarrStream*
setup()
{
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .multiscale = true,
        .data_type = ZarrDataType_uint16,
        .version = ZarrVersion_3,
        .max_threads = 0, // use all available threads
        .downsampling_method = ZarrDownsamplingMethod_Mean,
    };

    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 2,
        .shuffle = 2,
    };
    settings.compression_settings = &compression_settings;

    CHECK_OK(ZarrStreamSettings_create_dimension_array(&settings, 4));

    ZarrDimensionProperties* dim;
    dim = settings.dimensions;
    *dim = DIM("t",
               ZarrDimensionType_Time,
               array_timepoints,
               chunk_timepoints,
               shard_timepoints,
               nullptr,
               1.0);

    dim = settings.dimensions + 1;
    *dim = DIM("c",
               ZarrDimensionType_Channel,
               array_channels,
               chunk_channels,
               shard_channels,
               nullptr,
               1.0);

    dim = settings.dimensions + 2;
    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               chunk_height,
               shard_height,
               "micrometer",
               0.9);

    dim = settings.dimensions + 3;
    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               chunk_width,
               shard_width,
               "micrometer",
               0.9);

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

    // OME metadata
    const auto ome = meta["attributes"]["ome"];
    const auto multiscales = ome["multiscales"][0];
    const auto ngff_version = ome["version"].get<std::string>();
    EXPECT(ngff_version == "0.5",
           "Expected version to be '0.5', but got '",
           ngff_version,
           "'");

    const auto axes = multiscales["axes"];
    EXPECT_EQ(size_t, axes.size(), 4); // Now 4 axes

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
    EXPECT(name == "y", "Expected name to be 'y', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '",
           unit,
           "'");

    name = axes[3]["name"];
    type = axes[3]["type"];
    unit = axes[3]["unit"];
    EXPECT(name == "x", "Expected name to be 'x', but got '", name, "'");
    EXPECT(
      type == "space", "Expected type to be 'space', but got '", type, "'");
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '",
           unit,
           "'");

    const auto datasets = multiscales["datasets"];
    for (auto level = 0; level < 3; ++level) {
        const auto& dataset = datasets[level];

        const std::string path = dataset["path"].get<std::string>();
        EXPECT(path == std::to_string(level),
               "Expected path to be ',",
               std::to_string(level),
               "', but got '",
               path,
               "'");

        const auto coordinate_transformations =
          dataset["coordinateTransformations"];

        type = coordinate_transformations[0]["type"].get<std::string>();
        EXPECT(
          type == "scale", "Expected type to be 'scale', but got '", type, "'");

        const auto scale = coordinate_transformations[0]["scale"];
        EXPECT_EQ(size_t, scale.size(), 4); // Now 4 scale factors
        EXPECT_EQ(double, scale[0].get<double>(), 1.0);
        EXPECT_EQ(double, scale[1].get<double>(), 1.0);
        EXPECT_EQ(double, scale[2].get<double>(), std::pow(2, level) * 0.9);
        EXPECT_EQ(double, scale[3].get<double>(), std::pow(2, level) * 0.9);
    }
}

void
verify_array_metadata(const nlohmann::json& meta, int level)
{
    const auto acquired_frames = static_cast<double>(frames_to_acquire);
    const auto expected_array_width =
      static_cast<uint32_t>(std::ceil(array_width / std::pow(2, level)));
    const auto expected_array_height =
      static_cast<uint32_t>(std::ceil(array_height / std::pow(2, level)));
    const auto expected_array_timepoints =
      static_cast<uint32_t>(std::ceil(acquired_frames / array_channels));

    const auto expected_chunk_height =
      std::min(chunk_height, expected_array_height);
    const auto expected_chunk_width =
      std::min(chunk_width, expected_array_width);

    const auto expected_shard_height =
      std::min(expected_array_height, expected_chunk_height * shard_height);
    const auto expected_shard_width =
      std::min(expected_array_width, expected_chunk_width * shard_width);

    const auto& shape = meta["shape"];
    EXPECT_EQ(size_t, shape.size(), 4);
    EXPECT_EQ(int, shape[0].get<int>(), expected_array_timepoints);
    EXPECT_EQ(int, shape[1].get<int>(), array_channels);
    EXPECT_EQ(int, shape[2].get<int>(), expected_array_height);
    EXPECT_EQ(int, shape[3].get<int>(), expected_array_width);

    const auto& chunks = meta["chunk_grid"]["configuration"]["chunk_shape"];
    EXPECT_EQ(size_t, chunks.size(), 4);
    EXPECT_EQ(int, chunks[0].get<int>(), chunk_timepoints* shard_timepoints);
    EXPECT_EQ(int, chunks[1].get<int>(), chunk_channels* shard_channels);
    EXPECT_EQ(int, chunks[2].get<int>(), expected_shard_height);
    EXPECT_EQ(int, chunks[3].get<int>(), expected_shard_width);

    const auto dtype = meta["data_type"].get<std::string>();
    EXPECT(dtype == "uint16",
           "Expected dtype to be 'uint16', but got '",
           dtype,
           "'");

    const auto& codecs = meta["codecs"];
    EXPECT_EQ(size_t, codecs.size(), 1);
    const auto& sharding_codec = codecs[0]["configuration"];

    const auto& shards = sharding_codec["chunk_shape"];
    EXPECT_EQ(size_t, shards.size(), 4);
    EXPECT_EQ(int, shards[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, shards[1].get<int>(), chunk_channels);
    EXPECT_EQ(int, shards[2].get<int>(), expected_chunk_height);
    EXPECT_EQ(int, shards[3].get<int>(), expected_chunk_width);

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
verify_file_data(int level)
{
    const auto acquired_frames = frames_to_acquire / std::pow(2, level);
    const auto expected_array_width =
      static_cast<uint32_t>(std::ceil(array_width / std::pow(2, level)));
    const auto expected_array_height =
      static_cast<uint32_t>(std::ceil(array_height / std::pow(2, level)));
    const auto expected_array_timepoints =
      static_cast<uint32_t>(std::ceil(acquired_frames / array_channels));

    const auto expected_chunk_height =
      std::min(chunk_height, expected_array_height);
    const auto expected_chunk_width =
      std::min(chunk_width, expected_array_width);

    const auto expected_chunks_in_x =
      (expected_array_width + expected_chunk_width - 1) / expected_chunk_width;
    const auto expected_chunks_in_y =
      (expected_array_height + expected_chunk_height - 1) /
      expected_chunk_height;
    const auto expected_chunks_in_t =
      (expected_array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

    const auto expected_shards_in_x =
      (expected_chunks_in_x + shard_width - 1) / shard_width;
    const auto expected_shards_in_y =
      (expected_chunks_in_y + shard_height - 1) / shard_height;
    const unsigned int expected_shards_in_t =
      (expected_chunks_in_t + shard_timepoints - 1) / shard_timepoints;

    const auto expected_chunk_size = expected_chunk_width *
                                     expected_chunk_height * chunk_channels *
                                     chunk_timepoints * nbytes_px;

    const auto index_size = chunks_per_shard *
                            sizeof(uint64_t) * // indices are 64 bits
                            2;                 // 2 indices per chunk
    const auto checksum_size = 4;              // crc32 checksum is 4 bytes
    const auto expected_file_size = shard_width * shard_height *
                                      shard_channels * shard_timepoints *
                                      expected_chunk_size +
                                    index_size + checksum_size;

    fs::path data_root = fs::path(test_path) / std::to_string(level);

    CHECK(fs::is_directory(data_root));
    for (auto t = 0; t < expected_shards_in_t; ++t) {
        const auto t_dir = data_root / "c" / std::to_string(t);
        CHECK(fs::is_directory(t_dir));

        for (auto c = 0; c < shards_in_c; ++c) {
            const auto c_dir = t_dir / std::to_string(c);
            CHECK(fs::is_directory(c_dir));

            for (auto y = 0; y < expected_shards_in_y; ++y) {
                const auto y_dir = c_dir / std::to_string(y);
                CHECK(fs::is_directory(y_dir));

                for (auto x = 0; x < expected_shards_in_x; ++x) {
                    const auto x_file = y_dir / std::to_string(x);
                    EXPECT(fs::is_regular_file(x_file),
                           "Missing file '",
                           x_file.string(),
                           "'");
                    const auto file_size = fs::file_size(x_file);
                    EXPECT(file_size < expected_file_size,
                           "Expected file size < ",
                           expected_file_size,
                           " for file ",
                           x_file.string(),
                           ", got ",
                           file_size);
                }

                CHECK(!fs::is_regular_file(
                  y_dir / std::to_string(expected_shards_in_x)));
            }

            CHECK(
              !fs::is_directory(c_dir / std::to_string(expected_shards_in_y)));
        }

        CHECK(!fs::is_directory(t_dir / std::to_string(shards_in_c)));
    }

    CHECK(!fs::is_directory(data_root / "c" /
                            std::to_string(expected_shards_in_t)));
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

    for (auto level = 0; level < 3; ++level) {
        fs::path array_metadata_path =
          fs::path(test_path) / std::to_string(level) / "zarr.json";
        EXPECT(fs::is_regular_file(array_metadata_path),
               "Expected file '",
               array_metadata_path,
               "' to exist");
        std::ifstream f = std::ifstream(array_metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(f);

        verify_array_metadata(array_metadata, level);

        verify_file_data(level);
    }
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
