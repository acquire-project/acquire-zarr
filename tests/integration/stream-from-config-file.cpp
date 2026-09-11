// Load stream settings from YAML and JSON config files, round-trip via dump,
// and confirm the loaded settings drive a working stream.
// @see config-file support
#include "acquire.zarr.h"
#include "test.macros.hh"

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {
const fs::path store = "config-test.zarr";

auto config_yaml = R"(version: 1
store_path: config-test.zarr
overwrite: true
max_threads: 4
ome_version: "0.6"
arrays:
  - data_type: uint16
    compression:
      compressor: blosc1
      codec: blosc-zstd
      level: 1
      shuffle: 1
    omero:
      id: 1
      name: my image
      channels:
        - {label: red, color: "FF0000", window: {min: 0.0, max: 65535.0, start: 0.0, end: 1500.0}, active: true, coefficient: 1.0}
        - {label: green, color: "00FF00", window: {min: 0.0, max: 65535.0, start: 0.0, end: 2000.0}, active: false}
      rdefs: {model: color, defaultT: 0, defaultZ: 2}
    dimensions:
      - {name: t, type: time,  array_size_px: 0,  chunk_size_px: 5,  shard_size_chunks: 1}
      - {name: c, type: channel, array_size_px: 2, chunk_size_px: 1, shard_size_chunks: 1}
      - {name: z, type: space, array_size_px: 10, chunk_size_px: 5,  shard_size_chunks: 1}
      - {name: y, type: space, array_size_px: 48, chunk_size_px: 16, shard_size_chunks: 1, unit: micrometer, scale: 0.5}
      - {name: x, type: space, array_size_px: 64, chunk_size_px: 16, shard_size_chunks: 1, unit: micrometer, scale: 0.5}
)";

auto config_json = R"({
  "version": 1,
  "store_path": "config-test.zarr",
  "overwrite": true,
  "max_threads": 4,
  "ome_version": "0.6",
  "arrays": [
    {
      "data_type": "uint16",
      "compression": {"compressor": "blosc1", "codec": "blosc-zstd", "level": 1, "shuffle": 1},
      "omero": {
        "id": 1,
        "name": "my image",
        "channels": [
          {"label": "red", "color": "FF0000", "window": {"min": 0.0, "max": 65535.0, "start": 0.0, "end": 1500.0}, "active": true, "coefficient": 1.0},
          {"label": "green", "color": "00FF00", "window": {"min": 0.0, "max": 65535.0, "start": 0.0, "end": 2000.0}, "active": false}
        ],
        "rdefs": {"model": "color", "defaultT": 0, "defaultZ": 2}
      },
      "dimensions": [
        {"name": "t", "type": "time",  "array_size_px": 0,  "chunk_size_px": 5,  "shard_size_chunks": 1},
        {"name": "c", "type": "channel", "array_size_px": 2, "chunk_size_px": 1, "shard_size_chunks": 1},
        {"name": "z", "type": "space", "array_size_px": 10, "chunk_size_px": 5,  "shard_size_chunks": 1},
        {"name": "y", "type": "space", "array_size_px": 48, "chunk_size_px": 16, "shard_size_chunks": 1, "unit": "micrometer", "scale": 0.5},
        {"name": "x", "type": "space", "array_size_px": 64, "chunk_size_px": 16, "shard_size_chunks": 1, "unit": "micrometer", "scale": 0.5}
      ]
    }
  ]
})";

void
assert_expected(const ZarrStreamSettings& s)
{
    EXPECT_STR_EQ(s.store_path, "config-test.zarr");
    CHECK(s.overwrite);
    EXPECT_EQ(unsigned, s.max_threads, 4u);
    EXPECT_EQ(int, s.ome_version, ZarrOMEVersion_0_6);
    EXPECT_EQ(size_t, s.array_count, 1u);
    CHECK(s.s3_settings == nullptr);
    CHECK(s.hcs_settings == nullptr);

    const auto& a = s.arrays[0];
    CHECK(a.output_key == nullptr);
    EXPECT_EQ(int, a.data_type, ZarrDataType_uint16);
    CHECK(!a.multiscale);

    CHECK(a.omero != nullptr);
    CHECK(a.omero->has_id);
    EXPECT_EQ(uint32_t, a.omero->id, 1u);
    EXPECT_STR_EQ(a.omero->name, "my image");
    EXPECT_EQ(size_t, a.omero->channel_count, 2u);

    EXPECT_STR_EQ(a.omero->channels[0].label, "red");
    EXPECT_STR_EQ(a.omero->channels[0].color, "FF0000");
    CHECK(a.omero->channels[0].active);
    CHECK(a.omero->channels[0].window.max == 65535.0);
    CHECK(a.omero->channels[0].window.end == 1500.0);
    CHECK(a.omero->channels[0].has_coefficient);
    CHECK(a.omero->channels[0].coefficient == 1.0);

    EXPECT_STR_EQ(a.omero->channels[1].label, "green");
    CHECK(!a.omero->channels[1].active);
    CHECK(!a.omero->channels[1].has_coefficient);

    CHECK(a.omero->has_rdefs);
    EXPECT_STR_EQ(a.omero->rdefs.model, "color");
    EXPECT_EQ(uint32_t, a.omero->rdefs.default_z, 2u);

    CHECK(a.compression_settings != nullptr);
    EXPECT_EQ(int, a.compression_settings->compressor, ZarrCompressor_Blosc1);
    EXPECT_EQ(
      int, a.compression_settings->codec, ZarrCompressionCodec_BloscZstd);
    EXPECT_EQ(int, a.compression_settings->level, 1);
    EXPECT_EQ(int, a.compression_settings->shuffle, 1);

    EXPECT_EQ(size_t, a.dimension_count, 5u);
    EXPECT_STR_EQ(a.dimensions[0].name, "t");
    EXPECT_EQ(int, a.dimensions[0].type, ZarrDimensionType_Time);
    EXPECT_EQ(uint32_t, a.dimensions[0].chunk_size_px, 5u);
    EXPECT_STR_EQ(a.dimensions[1].name, "c");
    EXPECT_EQ(int, a.dimensions[1].type, ZarrDimensionType_Channel);
    EXPECT_STR_EQ(a.dimensions[3].name, "y");
    EXPECT_STR_EQ(a.dimensions[3].unit, "micrometer");
    CHECK(a.dimensions[3].scale == 0.5);
    CHECK(a.storage_dimension_order == nullptr);
}

void
run_stream(ZarrStreamSettings* settings)
{
    if (fs::exists(store)) {
        fs::remove_all(store);
    }

    ZarrStream* stream = ZarrStream_create(settings);
    CHECK(stream != nullptr);

    std::vector<uint16_t> frame(48 * 64, 7);
    size_t bytes_out = 0;
    for (int i = 0; i < 20; ++i) { // both channels of one z-stack at t=0
        CHECK_OK(ZarrStream_append(stream,
                                   frame.data(),
                                   frame.size() * sizeof(uint16_t),
                                   &bytes_out,
                                   nullptr));
    }

    CHECK_OK(ZarrStream_close(stream));
    CHECK(fs::exists(store));
    CHECK(fs::exists(store / "zarr.json"));
    fs::remove_all(store);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        // both formats produce identical settings
        ZarrStreamSettings from_yaml{}, from_json{};
        CHECK_OK(ZarrStreamSettings_load_from_string(&from_yaml, config_yaml));
        assert_expected(from_yaml);
        CHECK_OK(ZarrStreamSettings_load_from_string(&from_json, config_json));
        assert_expected(from_json);

        // round-trip through dump (YAML and JSON) reloads identically
        for (auto fmt : { ZarrConfigFormat_Yaml, ZarrConfigFormat_Json }) {
            char* text = nullptr;
            CHECK_OK(ZarrStreamSettings_dump_to_string(&from_yaml, &text, fmt));
            CHECK(text != nullptr);

            ZarrStreamSettings reloaded{};
            CHECK_OK(ZarrStreamSettings_load_from_string(&reloaded, text));
            assert_expected(reloaded);
            ZarrStreamSettings_destroy_loaded(&reloaded);
            free(text);
        }

        // an unquoted YAML ome_version decodes as a double, not a string
        ZarrStreamSettings unquoted{};
        CHECK_OK(ZarrStreamSettings_load_from_string(
          &unquoted,
          "version: 1\nstore_path: x\nome_version: 0.6\narrays:\n  - "
          "data_type: uint8\n    dimensions:\n      - {name: t, type: time, "
          "array_size_px: 0, chunk_size_px: 1, shard_size_chunks: 1}\n      - "
          "{name: y, type: space, array_size_px: 4, chunk_size_px: 4, "
          "shard_size_chunks: 1}\n      - {name: x, type: space, "
          "array_size_px: 4, chunk_size_px: 4, shard_size_chunks: 1}\n"));
        EXPECT_EQ(int, unquoted.ome_version, ZarrOMEVersion_0_6);
        ZarrStreamSettings_destroy_loaded(&unquoted);

        // loaded settings drive a working stream
        run_stream(&from_yaml);

        ZarrStreamSettings_destroy_loaded(&from_yaml);
        ZarrStreamSettings_destroy_loaded(&from_json);

        // malformed configs are rejected
        ZarrStreamSettings bad{};
        EXPECT(ZarrStreamSettings_load_from_string(
                 &bad, "version: 1\nstore_path: x\n") != ZarrStatusCode_Success,
               "Expected failure: no arrays or plates");
        EXPECT(ZarrStreamSettings_load_from_string(
                 &bad,
                 "store_path: x\narrays:\n  - data_type: float128\n    "
                 "dimensions: []\n") != ZarrStatusCode_Success,
               "Expected failure: bad data_type");

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    if (fs::exists(store)) {
        fs::remove_all(store);
    }
    return retval;
}
