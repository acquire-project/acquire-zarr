#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>
#include <turbojpeg.h>

#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();

// (t, c, z, y, x); level 1 is (2, 1, 8, 64, 64) with one (1, 1, 8, 64, 64)
// chunk per shard, which JPEG sees as one 512 x 64 image
const uint32_t array_t = 2, array_c = 1, array_z = 16, array_y = 128,
               array_x = 128;
const uint32_t chunk_z = 8, chunk_y = 64, chunk_x = 64;
const double window_max = 4095.0;

uint16_t
sample(uint32_t t, uint32_t z, uint32_t y, uint32_t x)
{
    const double v = 0.5 + 0.25 * std::sin(x / 9.0 + t) * std::cos(y / 11.0) +
                     0.2 * std::sin(z / 5.0);
    return static_cast<uint16_t>(std::lround(v * window_max));
}

ZarrJpegSettings jpeg_settings = {
    .quality = 90,
    .window_min = 0.0,
    .window_max = window_max,
};

ZarrStream*
create_stream(ZarrDataType dtype,
              const std::vector<ZarrDimensionProperties>& dims,
              ZarrJpegSettings* jpeg,
              ZarrDownsamplingMethod method = ZarrDownsamplingMethod_Mean)
{
    ZarrArraySettings array = {
        .data_type = dtype,
        .downsampling_method = method,
        .jpeg_settings = jpeg,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .max_threads = 0,
        .arrays = &array,
        .array_count = 1,
    };

    ZarrCompressionSettings compression = {
        .compressor = ZarrCompressor_Zstd,
        .codec = ZarrCompressionCodec_Zstd,
        .level = 1,
    };
    array.compression_settings = &compression;

    CHECK_OK(ZarrArraySettings_create_dimension_array(&array, dims.size()));
    std::memcpy(
      array.dimensions, dims.data(), dims.size() * sizeof(dims.front()));

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(&array);
    return stream;
}

std::vector<ZarrDimensionProperties>
five_dims(uint32_t chunk_t, uint32_t cz, uint32_t cy, uint32_t cx)
{
    return {
        DIM("t", ZarrDimensionType_Time, array_t, chunk_t, 1, nullptr, 1.0),
        DIM("c", ZarrDimensionType_Channel, array_c, 1, 1, nullptr, 1.0),
        DIM("z", ZarrDimensionType_Space, array_z, cz, 1, "micrometer", 1.0),
        DIM("y", ZarrDimensionType_Space, array_y, cy, 1, "micrometer", 1.0),
        DIM("x", ZarrDimensionType_Space, array_x, cx, 1, "micrometer", 1.0),
    };
}

void
expect_rejected(const char* what, ZarrStream* stream)
{
    if (stream) {
        ZarrStream_destroy(stream);
    }
    EXPECT(stream == nullptr, "Expected the stream to be rejected: ", what);
}

void
test_validation()
{
    // the image height is 32 * 1 * 16 * 128 = 65536, one over the JPEG limit
    {
        std::vector<ZarrDimensionProperties> dims = {
            DIM("t", ZarrDimensionType_Time, 64, 32, 1, nullptr, 1.0),
            DIM("c", ZarrDimensionType_Channel, 1, 1, 1, nullptr, 1.0),
            DIM("z", ZarrDimensionType_Space, 32, 16, 1, nullptr, 1.0),
            DIM("y", ZarrDimensionType_Space, 256, 128, 1, nullptr, 1.0),
            DIM("x", ZarrDimensionType_Space, 256, 128, 1, nullptr, 1.0),
        };
        expect_rejected(
          "65536 rows",
          create_stream(ZarrDataType_uint16, dims, &jpeg_settings));
    }

    expect_rejected("chunk not a multiple of 8",
                    create_stream(ZarrDataType_uint16,
                                  five_dims(1, chunk_z, 60, chunk_x),
                                  &jpeg_settings));

    expect_rejected("no downsampling",
                    create_stream(ZarrDataType_uint16,
                                  five_dims(1, chunk_z, chunk_y, chunk_x),
                                  &jpeg_settings,
                                  ZarrDownsamplingMethod_None));

    ZarrJpegSettings bad = jpeg_settings;
    bad.quality = 0;
    expect_rejected("quality 0",
                    create_stream(ZarrDataType_uint16,
                                  five_dims(1, chunk_z, chunk_y, chunk_x),
                                  &bad));

    bad = jpeg_settings;
    bad.window_min = bad.window_max;
    expect_rejected("empty window",
                    create_stream(ZarrDataType_uint16,
                                  five_dims(1, chunk_z, chunk_y, chunk_x),
                                  &bad));
}

void
test_config_round_trip()
{
    const char* yaml = R"(version: 2
store_path: jpeg-config-test.zarr
arrays:
  - data_type: uint16
    downsampling_method: mean
    jpeg: {quality: 85, window_min: 100, window_max: 4000}
    dimensions:
      - {name: z, type: space, array_size_px: 0, chunk_size_px: 8, shard_size_chunks: 1}
      - {name: y, type: space, array_size_px: 256, chunk_size_px: 64, shard_size_chunks: 1}
      - {name: x, type: space, array_size_px: 256, chunk_size_px: 64, shard_size_chunks: 1}
)";

    ZarrStreamSettings settings{};
    CHECK_OK(ZarrStreamSettings_load_from_string(&settings, yaml));
    const auto* js = settings.arrays[0].jpeg_settings;
    CHECK(js);
    EXPECT_EQ(int, js->quality, 85);
    EXPECT_EQ(double, js->window_min, 100.0);
    EXPECT_EQ(double, js->window_max, 4000.0);

    char* text = nullptr;
    CHECK_OK(ZarrStreamSettings_dump_to_string(
      &settings, &text, ZarrConfigFormat_Json));
    const auto dumped = nlohmann::json::parse(text);
    free(text);
    ZarrStreamSettings_destroy_loaded(&settings);

    const auto& jpeg = dumped["arrays"][0]["jpeg"];
    EXPECT_EQ(int, jpeg["quality"].get<int>(), 85);
    EXPECT_EQ(double, jpeg["window_min"].get<double>(), 100.0);
    EXPECT_EQ(double, jpeg["window_max"].get<double>(), 4000.0);
}

nlohmann::json
read_json(const fs::path& path)
{
    std::ifstream f(path);
    EXPECT(f.is_open(), "Failed to open ", path.string());
    return nlohmann::json::parse(f);
}

std::vector<uint8_t>
read_single_chunk_shard(const fs::path& path)
{
    std::ifstream f(path, std::ios::binary);
    EXPECT(f.is_open(), "Failed to open ", path.string());
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(f)),
                               std::istreambuf_iterator<char>());

    // index at the end: (offset, nbytes) as uint64, then a crc32c
    const size_t index_bytes = 2 * sizeof(uint64_t) + 4;
    EXPECT(bytes.size() > index_bytes, "Shard too small: ", path.string());
    uint64_t offset, nbytes;
    std::memcpy(&offset, bytes.data() + bytes.size() - index_bytes, 8);
    std::memcpy(&nbytes, bytes.data() + bytes.size() - index_bytes + 8, 8);
    EXPECT(offset + nbytes <= bytes.size() - index_bytes, "Bad shard index");

    return { bytes.begin() + offset, bytes.begin() + offset + nbytes };
}

void
verify_level0()
{
    const auto meta = read_json(fs::path(test_path) / "0" / "zarr.json");
    EXPECT_STR_EQ(meta["data_type"].get<std::string>().c_str(), "uint16");

    const auto codecs = meta["codecs"][0]["configuration"]["codecs"];
    EXPECT_EQ(size_t, codecs.size(), 2);
    EXPECT_STR_EQ(codecs[0]["name"].get<std::string>().c_str(), "bytes");
    EXPECT_STR_EQ(codecs[1]["name"].get<std::string>().c_str(), "zstd");
}

void
verify_level1()
{
    const auto level = fs::path(test_path) / "1";
    const auto meta = read_json(level / "zarr.json");
    // the level declares the data type of level 0; the chain maps it to uint8
    EXPECT_STR_EQ(meta["data_type"].get<std::string>().c_str(), "uint16");

    const auto& sharding = meta["codecs"][0]["configuration"];
    const auto chunk_shape = sharding["chunk_shape"];
    EXPECT(chunk_shape == nlohmann::json({ 1, 1, chunk_z, chunk_y, chunk_x }),
           "Unexpected chunk shape: ",
           chunk_shape.dump());

    const auto expected_codecs = nlohmann::json::parse(R"([
      {"name": "cast_value", "configuration": {"data_type": "float32"}},
      {"name": "scale_offset", "configuration": {"offset": 0.0}},
      {"name": "cast_value", "configuration": {
        "data_type": "uint8", "rounding": "nearest-even",
        "out_of_range": "clamp"}},
      {"name": "reshape", "configuration": {"shape": [[0, 1, 2, 3], [4]]}},
      {"name": "jpeg", "configuration": {
        "quality": 90, "encoded_color_space": "grayscale"}}
    ])");
    auto codecs = sharding["codecs"];

    // the scale is 255 / 4095 rounded to float32
    const double scale = codecs[1]["configuration"]["scale"].get<double>();
    EXPECT_EQ(float, scale, 255.0f / static_cast<float>(window_max));
    codecs[1]["configuration"].erase("scale");
    EXPECT(codecs == expected_codecs, "Unexpected codecs: ", codecs.dump());

    // decode the chunk at t = 0 and compare with the downsampled input
    const auto jpeg = read_single_chunk_shard(level / "c/0/0/0/0/0");
    auto* h = tj3Init(TJINIT_DECOMPRESS);
    CHECK(h);
    CHECK(tj3DecompressHeader(h, jpeg.data(), jpeg.size()) == 0);
    const int width = tj3Get(h, TJPARAM_JPEGWIDTH);
    const int height = tj3Get(h, TJPARAM_JPEGHEIGHT);
    EXPECT_EQ(int, width, chunk_x);
    EXPECT_EQ(int, height, chunk_z* chunk_y);
    EXPECT_EQ(int, tj3Get(h, TJPARAM_PRECISION), 8);
    EXPECT_EQ(int, tj3Get(h, TJPARAM_PROGRESSIVE), 0);

    std::vector<uint8_t> decoded(static_cast<size_t>(width) * height);
    CHECK(tj3Decompress8(
            h, jpeg.data(), jpeg.size(), decoded.data(), 0, TJPF_GRAY) == 0);
    tj3Destroy(h);

    double sq_err = 0.0;
    for (uint32_t z = 0; z < chunk_z; ++z) {
        for (uint32_t y = 0; y < chunk_y; ++y) {
            for (uint32_t x = 0; x < chunk_x; ++x) {
                double mean = 0.0;
                for (uint32_t k = 0; k < 8; ++k) {
                    mean += sample(0,
                                   2 * z + (k >> 2),
                                   2 * y + ((k >> 1) & 1),
                                   2 * x + (k & 1));
                }
                mean /= 8.0;
                const double expected = mean * 255.0 / window_max;
                const double got = decoded[(z * chunk_y + y) * chunk_x + x];
                sq_err += (got - expected) * (got - expected);
            }
        }
    }
    const double mse = sq_err / decoded.size();
    const double psnr = 10.0 * std::log10(255.0 * 255.0 / mse);
    LOG_INFO("Level 1 PSNR: ", psnr, " dB");
    EXPECT(psnr > 35.0, "PSNR too low: ", psnr, " dB");
}

void
verify_2d_level_codecs()
{
    std::vector<ZarrDimensionProperties> dims = {
        DIM("y", ZarrDimensionType_Space, 256, 64, 1, nullptr, 1.0),
        DIM("x", ZarrDimensionType_Space, 256, 64, 1, nullptr, 1.0),
    };
    auto* stream = create_stream(ZarrDataType_uint8, dims, &jpeg_settings);
    CHECK(stream);

    std::vector<uint8_t> frame(256 * 256);
    for (size_t i = 0; i < frame.size(); ++i) {
        frame[i] = static_cast<uint8_t>((i % 256 + i / 256) / 2);
    }
    size_t bytes_out;
    CHECK_OK(ZarrStream_append(
      stream, frame.data(), frame.size(), &bytes_out, nullptr));
    ZarrStream_destroy(stream);

    const auto meta = read_json(fs::path(test_path) / "1" / "zarr.json");
    const auto codecs = meta["codecs"][0]["configuration"]["codecs"];

    // reshape only when the level has more than 2 dimensions; the levels of a
    // 2D pyramid currently keep the phantom "_singleton" dimension
    const auto ndims = meta["shape"].size();
    EXPECT_EQ(size_t, codecs.size(), ndims > 2 ? 2 : 1);
    if (ndims > 2) {
        EXPECT_STR_EQ(codecs[0]["name"].get<std::string>().c_str(), "reshape");
    }
    EXPECT_STR_EQ(codecs.back()["name"].get<std::string>().c_str(), "jpeg");
}
} // namespace

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Info);

    int retval = 1;
    try {
        test_validation();
        test_config_round_trip();

        auto* stream = create_stream(ZarrDataType_uint16,
                                     five_dims(1, chunk_z, chunk_y, chunk_x),
                                     &jpeg_settings);
        CHECK(stream);

        std::vector<uint16_t> frame(array_y * array_x);
        for (uint32_t t = 0; t < array_t; ++t) {
            for (uint32_t z = 0; z < array_z; ++z) {
                for (uint32_t y = 0; y < array_y; ++y) {
                    for (uint32_t x = 0; x < array_x; ++x) {
                        frame[y * array_x + x] = sample(t, z, y, x);
                    }
                }
                size_t bytes_out;
                CHECK_OK(ZarrStream_append(stream,
                                           frame.data(),
                                           frame.size() * sizeof(uint16_t),
                                           &bytes_out,
                                           nullptr));
            }
        }
        ZarrStream_destroy(stream);

        verify_level0();
        verify_level1();
        fs::remove_all(test_path);

        verify_2d_level_codecs();
        fs::remove_all(test_path);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    return retval;
}
