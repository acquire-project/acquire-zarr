#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <vector>

namespace fs = std::filesystem;

namespace {
const fs::path test_path = fs::temp_directory_path() / (TEST ".zarr");

const unsigned int array_width = 64, array_height = 48, array_timepoints = 4;
const unsigned int chunk_width = 16, chunk_height = 16, chunk_timepoints = 2;

const size_t bytes_of_frame = array_width * array_height * sizeof(uint16_t);

using StreamPtr = std::unique_ptr<ZarrStream, decltype(&ZarrStream_destroy)>;

void
write_file(const fs::path& path, const std::string& contents)
{
    fs::create_directories(path.parent_path());
    std::ofstream f(path, std::ios::binary);
    f << contents;
}

nlohmann::json
read_metadata(const std::string& key)
{
    const fs::path path =
      (key.empty() ? test_path : test_path / key) / "zarr.json";
    EXPECT(fs::is_regular_file(path), "Expected ", path, " to exist");

    std::ifstream f(path, std::ios::binary);
    std::stringstream ss;
    ss << f.rdbuf();
    return nlohmann::json::parse(ss.str());
}

void
remove_test_path()
{
    std::error_code ec;
    fs::remove_all(test_path, ec);
    if (ec) {
        LOG_WARNING("Failed to remove ", test_path, ": ", ec.message());
    }
}

void
set_dimensions(ZarrArraySettings* array)
{
    CHECK_OK(ZarrArraySettings_create_dimension_array(array, 3));

    ZarrDimensionProperties* dim = array->dimensions;
    *dim = DIM("t",
               ZarrDimensionType_Time,
               array_timepoints,
               chunk_timepoints,
               1,
               nullptr,
               1.0);
    *(dim + 1) = DIM("y",
                     ZarrDimensionType_Space,
                     array_height,
                     chunk_height,
                     1,
                     nullptr,
                     1.0);
    *(dim + 2) = DIM(
      "x", ZarrDimensionType_Space, array_width, chunk_width, 1, nullptr, 1.0);
}

/// A multiscale array at foo/bar/baz and a flat array at foo/qux, so the
/// store has the groups "", foo and foo/bar.
ZarrStream*
create_stream(ZarrIntermediateGroups mode, bool overwrite)
{
    static const std::string store_path = test_path.string();
    ZarrArraySettings arrays[2] = {
        {
          .output_key = "foo/bar/baz",
          .data_type = ZarrDataType_uint16,
          .downsampling_method = ZarrDownsamplingMethod_Mean,
          .is_ngff = true,
        },
        {
          .output_key = "foo/qux",
          .data_type = ZarrDataType_uint16,
        },
    };
    ZarrStreamSettings settings = {
        .store_path = store_path.c_str(),
        .overwrite = overwrite,
        .arrays = arrays,
        .array_count = 2,
        .intermediate_groups = mode,
    };

    set_dimensions(arrays);
    set_dimensions(arrays + 1);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(arrays);
    ZarrArraySettings_destroy_dimension_array(arrays + 1);

    return stream;
}

void
append_frames(ZarrStream* stream)
{
    const std::vector<uint16_t> frame(array_width * array_height, 1);
    for (const char* key : { "foo/bar/baz", "foo/qux" }) {
        for (unsigned int i = 0; i < array_timepoints; ++i) {
            size_t bytes_out;
            CHECK_OK(ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, key));
        }
    }
}

void
expect_status(ZarrStream* stream,
              const char* key,
              const char* metadata_key,
              const char* metadata,
              ZarrStatusCode expected)
{
    const auto status =
      ZarrStream_write_custom_metadata(stream, key, metadata_key, metadata);
    EXPECT(status == expected,
           "Writing metadata to '",
           key ? key : "(NULL)",
           "': expected ",
           Zarr_get_status_message(expected),
           ", got ",
           Zarr_get_status_message(status));
}

void
test_every_node_is_addressable()
{
    LOG_INFO("Case: every node is addressable");
    remove_test_path();

    {
        StreamPtr stream(create_stream(ZarrIntermediateGroups_IfMissing, true),
                         &ZarrStream_destroy);
        EXPECT(stream != nullptr, "Failed to create stream");

        for (const char* key : { "",
                                 "foo",
                                 "foo/bar",
                                 "foo/bar/baz",
                                 "foo/bar/baz/0",
                                 "foo/bar/baz/1",
                                 "foo/qux" }) {
            const std::string metadata =
              nlohmann::json{ { "node", key } }.dump();
            expect_status(stream.get(),
                          key,
                          "custom",
                          metadata.c_str(),
                          ZarrStatusCode_Success);
        }

        // a key is regularized, as for arrays
        expect_status(
          stream.get(), "/foo/bar/", "extra", "1", ZarrStatusCode_Success);

        append_frames(stream.get());
    }

    for (const std::string key : { "",
                                   "foo",
                                   "foo/bar",
                                   "foo/bar/baz",
                                   "foo/bar/baz/0",
                                   "foo/bar/baz/1",
                                   "foo/qux" }) {
        const auto metadata = read_metadata(key);
        EXPECT(metadata["attributes"]["custom"]["node"] == key,
               "Expected custom metadata on '",
               key,
               "', got ",
               metadata.dump());
    }

    // the stream's own metadata is still there
    const auto root = read_metadata("");
    EXPECT(root["node_type"] == "group", "Root is not a group");
    EXPECT(root["consolidated_metadata"].is_null(),
           "Root has consolidated metadata");

    const auto bar = read_metadata("foo/bar");
    EXPECT(bar["attributes"]["extra"] == 1, "Expected 'extra' on foo/bar");

    const auto baz = read_metadata("foo/bar/baz");
    EXPECT(baz["attributes"].contains("ome"), "Multiscale group lost 'ome'");

    const auto level = read_metadata("foo/bar/baz/1");
    EXPECT(level["node_type"] == "array", "Level 1 is not an array");
}

void
test_unknown_keys_are_rejected()
{
    LOG_INFO("Case: unknown keys are rejected");
    remove_test_path();

    StreamPtr stream(create_stream(ZarrIntermediateGroups_IfMissing, true),
                     &ZarrStream_destroy);
    EXPECT(stream != nullptr, "Failed to create stream");

    for (const char* key : { "nope",
                             "foo/nope",
                             "foo/bar/baz/01", // not the path of level 1
                             "foo/bar/baz/99", // no such level
                             "foo/qux/0",      // not multiscale
                             "foo/bar/baz/0/x" }) {
        expect_status(
          stream.get(), key, "custom", "{}", ZarrStatusCode_KeyNotFound);
    }

    // two arrays, so a NULL key is ambiguous
    expect_status(
      stream.get(), nullptr, "custom", "{}", ZarrStatusCode_InvalidArgument);
}

void
test_ome_rules()
{
    LOG_INFO("Case: custom 'ome' metadata");
    remove_test_path();

    const char* omero = R"({"omero": {"channels": []}})";
    {
        StreamPtr stream(create_stream(ZarrIntermediateGroups_IfMissing, true),
                         &ZarrStream_destroy);
        EXPECT(stream != nullptr, "Failed to create stream");

        // `ome` is allowed on every node, as an object
        expect_status(
          stream.get(), "foo", "ome", omero, ZarrStatusCode_Success);
        expect_status(stream.get(),
                      "foo/qux",
                      nullptr,
                      R"({"ome": {"version": "0.6"}})",
                      ZarrStatusCode_Success);
        expect_status(stream.get(),
                      "foo/bar/baz",
                      "ome",
                      R"({"version": "0.5", "omero": {"channels": []}})",
                      ZarrStatusCode_Success);

        // `ome.multiscales` is refused on every node
        for (const char* key : { "", "foo", "foo/bar/baz", "foo/bar/baz/0" }) {
            expect_status(stream.get(),
                          key,
                          "ome",
                          R"({"multiscales": []})",
                          ZarrStatusCode_InvalidArgument);
            expect_status(stream.get(),
                          key,
                          nullptr,
                          R"({"ome": {"multiscales": []}})",
                          ZarrStatusCode_InvalidArgument);
        }

        // a key that the stream writes on that node is refused, except an
        // equal version
        expect_status(stream.get(),
                      "foo/bar/baz",
                      "ome",
                      R"({"version": "0.6"})",
                      ZarrStatusCode_InvalidArgument);
        expect_status(stream.get(),
                      "foo/bar/baz",
                      "ome",
                      R"({"name": "image"})",
                      ZarrStatusCode_InvalidArgument);

        // a refused call writes none of its keys
        expect_status(stream.get(),
                      "foo",
                      nullptr,
                      R"({"ome": {"multiscales": []}, "partial": 1})",
                      ZarrStatusCode_InvalidArgument);

        // not an object, and not a path
        expect_status(
          stream.get(), "foo", "ome", "[]", ZarrStatusCode_InvalidArgument);
        expect_status(stream.get(),
                      "foo",
                      nullptr,
                      R"({"ome": 1})",
                      ZarrStatusCode_InvalidArgument);
        expect_status(stream.get(),
                      "foo",
                      "ome/omero",
                      "{}",
                      ZarrStatusCode_InvalidArgument);

        expect_status(stream.get(),
                      "foo",
                      "custom",
                      "not json",
                      ZarrStatusCode_InvalidArgument);

        append_frames(stream.get());
    }

    // a refused call leaves what was accepted before it
    const auto foo = read_metadata("foo");
    EXPECT(foo["attributes"]["ome"]["omero"]["channels"].is_array(),
           "Expected 'ome.omero' on foo: ",
           foo.dump());
    EXPECT(!foo["attributes"]["ome"].contains("multiscales"),
           "Unexpected 'ome.multiscales' on foo: ",
           foo.dump());
    EXPECT(!foo["attributes"].contains("partial"),
           "A refused call wrote 'partial' on foo: ",
           foo.dump());

    const auto qux = read_metadata("foo/qux");
    EXPECT(qux["attributes"]["ome"]["version"] == "0.6",
           "Expected 'ome.version' on foo/qux: ",
           qux.dump());

    // the custom keys merge with the stream's own
    const auto baz = read_metadata("foo/bar/baz");
    const auto& ome = baz["attributes"]["ome"];
    EXPECT(ome["version"] == "0.5", "Unexpected version: ", baz.dump());
    EXPECT(ome["multiscales"].is_array() && ome["multiscales"].size() == 1,
           "Expected the stream's multiscales: ",
           baz.dump());
    EXPECT(ome["omero"]["channels"].is_array(),
           "Expected 'ome.omero' on foo/bar/baz: ",
           baz.dump());
}

void
test_never_groups_are_not_addressable()
{
    LOG_INFO("Case: groups not written by the stream are not addressable");
    remove_test_path();

    StreamPtr stream(create_stream(ZarrIntermediateGroups_Never, true),
                     &ZarrStream_destroy);
    EXPECT(stream != nullptr, "Failed to create stream");

    for (const char* key : { "", "foo", "foo/bar" }) {
        expect_status(
          stream.get(), key, "custom", "{}", ZarrStatusCode_InvalidArgument);
    }

    // arrays and levels are still addressable
    expect_status(
      stream.get(), "foo/bar/baz/1", "custom", "{}", ZarrStatusCode_Success);
}

void
test_kept_group_keeps_its_attributes()
{
    LOG_INFO("Case: a kept group keeps its attributes");
    remove_test_path();

    write_file(test_path / "foo" / "zarr.json", R"({
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {
            "written_by": "caller",
            "ome": { "version": "0.5", "kept": true }
        }
    })");

    {
        StreamPtr stream(create_stream(ZarrIntermediateGroups_IfMissing, false),
                         &ZarrStream_destroy);
        EXPECT(stream != nullptr, "Failed to create stream");

        expect_status(stream.get(),
                      "foo",
                      nullptr,
                      R"({"added_by": "stream"})",
                      ZarrStatusCode_Success);

        // the kept group's `ome` is the caller's, so no key of it is refused;
        // the custom keys merge into it
        expect_status(stream.get(),
                      "foo",
                      "ome",
                      R"({"version": "0.6", "added": true})",
                      ZarrStatusCode_Success);
        append_frames(stream.get());
    }

    const auto foo = read_metadata("foo");
    EXPECT(foo["attributes"]["written_by"] == "caller",
           "Kept group lost its attributes: ",
           foo.dump());
    EXPECT(foo["attributes"]["added_by"] == "stream",
           "Kept group has no custom metadata: ",
           foo.dump());
    const auto& ome = foo["attributes"]["ome"];
    EXPECT(ome["kept"] == true && ome["added"] == true &&
             ome["version"] == "0.6",
           "Unexpected 'ome' on the kept group: ",
           foo.dump());
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        test_every_node_is_addressable();
        test_unknown_keys_are_rejected();
        test_ome_rules();
        test_never_groups_are_not_addressable();
        test_kept_group_keeps_its_attributes();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    remove_test_path();

    return retval;
}
