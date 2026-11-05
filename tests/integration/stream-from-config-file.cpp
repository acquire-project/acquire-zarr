// Load stream settings from YAML and JSON config files, round-trip via dump,
// and confirm the loaded settings drive a working stream.
// @see config-file support
#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {
const fs::path store = "config-test.zarr";

auto config_yaml = R"(version: 3
store_path: config-test.zarr
overwrite: true
max_threads: 4
arrays:
  - data_type: uint16
    compression:
      compressor: blosc1
      codec: blosc-zstd
      level: 1
      shuffle: 1
    dimensions:
      - {name: t, type: time,  array_size_px: 0,  chunk_size_px: 5,  shard_size_chunks: 1}
      - {name: z, type: space, array_size_px: 10, chunk_size_px: 5,  shard_size_chunks: 1}
      - {name: y, type: space, array_size_px: 48, chunk_size_px: 16, shard_size_chunks: 1, unit: micrometer, scale: 0.5}
      - {name: x, type: space, array_size_px: 64, chunk_size_px: 16, shard_size_chunks: 1, unit: micrometer, scale: 0.5}
)";

auto config_json = R"({
  "version": 3,
  "store_path": "config-test.zarr",
  "overwrite": true,
  "max_threads": 4,
  "arrays": [
    {
      "data_type": "uint16",
      "compression": {"compressor": "blosc1", "codec": "blosc-zstd", "level": 1, "shuffle": 1},
      "dimensions": [
        {"name": "t", "type": "time",  "array_size_px": 0,  "chunk_size_px": 5,  "shard_size_chunks": 1},
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
    EXPECT_EQ(size_t, s.array_count, 1u);
    CHECK(s.s3_settings == nullptr);

    const auto& a = s.arrays[0];
    CHECK(a.output_key == nullptr);
    EXPECT_EQ(int, a.data_type, ZarrDataType_uint16);
    CHECK(!a.is_ngff);
    EXPECT_EQ(int, a.downsampling_method, ZarrDownsamplingMethod_None);

    CHECK(a.compression_settings != nullptr);
    EXPECT_EQ(int, a.compression_settings->compressor, ZarrCompressor_Blosc1);
    EXPECT_EQ(
      int, a.compression_settings->codec, ZarrCompressionCodec_BloscZstd);
    EXPECT_EQ(int, a.compression_settings->level, 1);
    EXPECT_EQ(int, a.compression_settings->shuffle, 1);

    EXPECT_EQ(size_t, a.dimension_count, 4u);
    EXPECT_STR_EQ(a.dimensions[0].name, "t");
    EXPECT_EQ(int, a.dimensions[0].type, ZarrDimensionType_Time);
    EXPECT_EQ(uint32_t, a.dimensions[0].chunk_size_px, 5u);
    EXPECT_STR_EQ(a.dimensions[2].name, "y");
    EXPECT_STR_EQ(a.dimensions[2].unit, "micrometer");
    CHECK(a.dimensions[2].scale == 0.5);
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
    for (int i = 0; i < 10; ++i) { // one full z-stack at t=0
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

// An HCS plate in one config file: the fields of view are arrays, and the
// plate and well metadata are attributes that the stream writes at create
const char* hcs_config = R"(version: 3
store_path: config-hcs-test.zarr
overwrite: true
arrays:
  - output_key: plate/A/1/0
    is_ngff: true
    data_type: uint8
    dimensions:
      - {name: t, type: time, array_size_px: 0, chunk_size_px: 1, shard_size_chunks: 1}
      - {name: y, type: space, array_size_px: 32, chunk_size_px: 32, shard_size_chunks: 1}
      - {name: x, type: space, array_size_px: 32, chunk_size_px: 32, shard_size_chunks: 1}
attributes:
  - node_key: plate
    key: ome
    value:
      version: "0.5"
      plate:
        rows: [{name: A}]
        columns: [{name: "1"}]
        wells: [{path: A/1, rowIndex: 0, columnIndex: 0}]
  - node_key: plate/A/1
    key: ome
    value: {version: "0.5", well: {images: [{path: "0"}]}}
  - node_key: plate/A/1/0
    key: acquisition
    value: "first"
)";

nlohmann::json
read_json(const fs::path& path)
{
    std::ifstream f(path);
    CHECK(f.is_open());
    return nlohmann::json::parse(f);
}

void
test_attributes_from_config()
{
    const fs::path hcs_store = "config-hcs-test.zarr";

    // a reader of version 2 would drop the key, so version 2 refuses it
    std::string version_2(hcs_config);
    version_2.replace(0, strlen("version: 3"), "version: 2");
    ZarrStreamSettings refused{};
    EXPECT(ZarrStreamSettings_load_from_string(&refused, version_2.c_str()) !=
             ZarrStatusCode_Success,
           "Expected 'attributes' in a version 2 config to be refused");

    ZarrStreamSettings settings{};
    CHECK_OK(ZarrStreamSettings_load_from_string(&settings, hcs_config));
    EXPECT_EQ(size_t, settings.attributes_count, 3u);
    EXPECT_STR_EQ(settings.attributes[0].node_key, "plate");
    EXPECT_STR_EQ(settings.attributes[0].key, "ome");
    EXPECT_STR_EQ(settings.attributes[2].value, R"("first")");

    // a dump keeps each entry, with its metadata as a mapping
    char* yaml = nullptr;
    CHECK_OK(ZarrStreamSettings_dump_to_string(
      &settings, &yaml, ZarrConfigFormat_Yaml));
    ZarrStreamSettings reloaded{};
    CHECK_OK(ZarrStreamSettings_load_from_string(&reloaded, yaml));
    std::free(yaml);
    EXPECT_EQ(size_t, reloaded.attributes_count, 3u);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT(nlohmann::json::parse(reloaded.attributes[i].value) ==
                 nlohmann::json::parse(settings.attributes[i].value),
               "attributes[",
               i,
               "] changed in a round trip");
    }
    ZarrStreamSettings_destroy_loaded(&reloaded);

    // the stream writes the entries before ZarrStream_create returns
    ZarrStream* stream = ZarrStream_create(&settings);
    CHECK(stream != nullptr);

    auto plate = read_json(hcs_store / "plate" / "zarr.json");
    EXPECT(plate["attributes"]["ome"]["plate"]["wells"][0]["path"] == "A/1",
           "Expected plate metadata at create: ",
           plate.dump());
    auto well = read_json(hcs_store / "plate" / "A" / "1" / "zarr.json");
    EXPECT(well["attributes"]["ome"]["well"]["images"][0]["path"] == "0",
           "Expected well metadata at create: ",
           well.dump());

    std::vector<uint8_t> frame(32 * 32, 1);
    size_t bytes_out = 0;
    CHECK_OK(ZarrStream_append(
      stream, frame.data(), frame.size(), &bytes_out, nullptr));
    CHECK_OK(ZarrStream_close(stream));

    auto field = read_json(hcs_store / "plate" / "A" / "1" / "0" / "zarr.json");
    EXPECT(field["attributes"]["acquisition"] == "first",
           "Expected the field's attributes: ",
           field.dump());
    EXPECT(field["attributes"]["ome"].contains("multiscales"),
           "Expected the field to be a multiscales group: ",
           field.dump());

    // an entry with an unknown node fails create before the store changes:
    // the store from the first run, which overwrite would remove, is intact.
    // The loader owns the original string, so put it back before the free.
    const char* well_key = settings.attributes[1].node_key;
    settings.attributes[1].node_key = "plate/Z/9";
    stream = ZarrStream_create(&settings);
    settings.attributes[1].node_key = well_key;
    EXPECT(stream == nullptr, "Expected create to fail for an unknown node");
    CHECK(
      fs::exists(hcs_store / "plate" / "A" / "1" / "0" / "0" / "zarr.json"));
    EXPECT(read_json(hcs_store / "plate" / "zarr.json")["attributes"]["ome"]
             .contains("plate"),
           "A failed create changed the store");

    // a dump refuses metadata that is not JSON, which would otherwise reload
    // as a valid JSON string
    const char* metadata = settings.attributes[2].value;
    settings.attributes[2].value = "first";
    char* text = nullptr;
    EXPECT(ZarrStreamSettings_dump_to_string(
             &settings, &text, ZarrConfigFormat_Yaml) != ZarrStatusCode_Success,
           "Expected a dump of metadata that is not JSON to fail");
    settings.attributes[2].value = metadata;

    // with no key, an entry's value must be an object; checked before the
    // store changes
    const char* entry_key = settings.attributes[2].key;
    settings.attributes[2].key = nullptr;
    stream = ZarrStream_create(&settings);
    settings.attributes[2].key = entry_key;
    EXPECT(stream == nullptr,
           "Expected create to fail for a non-object value with no key");
    CHECK(
      fs::exists(hcs_store / "plate" / "A" / "1" / "0" / "0" / "zarr.json"));

    // a NULL entry list, and a NULL node key with more than one array
    ZarrStreamSettings bad = settings;
    bad.attributes = nullptr;
    EXPECT(ZarrStream_create(&bad) == nullptr,
           "Expected create to fail for a NULL attributes");

    ZarrArraySettings two_arrays[2] = { settings.arrays[0],
                                        settings.arrays[0] };
    two_arrays[1].output_key = "plate/A/1/1";
    bad = settings;
    bad.arrays = two_arrays;
    bad.array_count = 2;
    ZarrAttributes no_key = { .value = R"({"a": 1})" };
    bad.attributes = &no_key;
    bad.attributes_count = 1;
    EXPECT(ZarrStream_create(&bad) == nullptr,
           "Expected create to fail for a NULL node key with two arrays");

    ZarrStreamSettings_destroy_loaded(&settings);
    fs::remove_all(hcs_store);
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

        // loaded settings drive a working stream
        run_stream(&from_yaml);

        ZarrStreamSettings_destroy_loaded(&from_yaml);
        ZarrStreamSettings_destroy_loaded(&from_json);

        // malformed configs are rejected
        ZarrStreamSettings bad{};
        EXPECT(ZarrStreamSettings_load_from_string(
                 &bad, "version: 2\nstore_path: x\n") != ZarrStatusCode_Success,
               "Expected failure: no arrays");
        EXPECT(ZarrStreamSettings_load_from_string(
                 &bad,
                 "store_path: x\narrays:\n  - data_type: float128\n    "
                 "dimensions: []\n") != ZarrStatusCode_Success,
               "Expected failure: bad data_type");

        EXPECT(ZarrStreamSettings_load_from_string(
                 &bad, "version: 99\nstore_path: x\n") !=
                 ZarrStatusCode_Success,
               "Expected failure: unsupported schema version");

        // HCS plate settings were removed in schema version 3; a config of
        // any version that has them is refused, not partly loaded
        for (const char* version :
             { "version: 1", "version: 2", "version: 3" }) {
            std::string with_plates(config_yaml);
            with_plates.replace(
              with_plates.find("version: 3"), strlen("version: 3"), version);
            with_plates += "plates:\n  - path: p\n";
            EXPECT(ZarrStreamSettings_load_from_string(
                     &bad, with_plates.c_str()) != ZarrStatusCode_Success,
                   "Expected failure: 'plates' in a config declaring ",
                   version);
        }

        // a version 2 config without plates still loads
        std::string version_2(config_yaml);
        version_2.replace(
          version_2.find("version: 3"), strlen("version: 3"), "version: 2");
        ZarrStreamSettings from_v2{};
        CHECK_OK(
          ZarrStreamSettings_load_from_string(&from_v2, version_2.c_str()));
        assert_expected(from_v2);
        ZarrStreamSettings_destroy_loaded(&from_v2);

        // `multiscale` was replaced by `is_ngff` in schema version 2; a
        // version 2 config carrying it must be rejected, not reinterpreted
        std::string legacy(version_2);
        legacy.insert(legacy.find("  - data_type: uint16") +
                        strlen("  - data_type: uint16"),
                      "\n    multiscale: true");
        EXPECT(ZarrStreamSettings_load_from_string(&bad, legacy.c_str()) !=
                 ZarrStatusCode_Success,
               "Expected failure: 'multiscale' replaced by 'is_ngff'");

        // the same config declared as version 1 still loads, translated to
        // version 1 semantics: `multiscale` with no method meant Decimate
        legacy.replace(
          legacy.find("version: 2"), strlen("version: 2"), "version: 1");
        ZarrStreamSettings migrated{};
        CHECK_OK(
          ZarrStreamSettings_load_from_string(&migrated, legacy.c_str()));
        CHECK(migrated.array_count == 1);
        CHECK(migrated.arrays[0].is_ngff);
        EXPECT_EQ(int,
                  migrated.arrays[0].downsampling_method,
                  ZarrDownsamplingMethod_Decimate);
        ZarrStreamSettings_destroy_loaded(&migrated);

        test_attributes_from_config();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    if (fs::exists(store)) {
        fs::remove_all(store);
    }
    return retval;
}
