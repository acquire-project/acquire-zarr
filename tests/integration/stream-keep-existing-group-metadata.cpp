#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <vector>

namespace fs = std::filesystem;

namespace {
const fs::path test_path = fs::temp_directory_path() / (TEST ".zarr");

const unsigned int array_width = 32, array_height = 24, array_timepoints = 4;
const unsigned int chunk_width = 16, chunk_height = 16, chunk_timepoints = 2;

const size_t bytes_of_frame = array_width * array_height * sizeof(uint16_t);

// Group metadata written before the stream opens, as a caller that builds
// the hierarchy itself (e.g. with yaozarrs) would do.
const std::string root_metadata = R"({
    "zarr_format": 3,
    "node_type": "group",
    "attributes": { "written_by": "caller", "level": "root" }
})";
const std::string path_metadata = R"({
    "zarr_format": 3,
    "node_type": "group",
    "attributes": { "written_by": "caller", "level": "path" }
})";

// Not a group: an array node left by an earlier run, and a file cut short by a
// crash.
const std::string stale_array_metadata = R"({
    "zarr_format": 3,
    "node_type": "array",
    "attributes": {}
})";
const std::string truncated_metadata = R"({ "zarr_format": 3, "node_)";

using StreamPtr = std::unique_ptr<ZarrStream, decltype(&ZarrStream_destroy)>;

void
write_file(const fs::path& path, const std::string& contents)
{
    fs::create_directories(path.parent_path());
    std::ofstream f(path, std::ios::binary);
    f << contents;
}

std::string
read_file(const fs::path& path)
{
    std::ifstream f(path, std::ios::binary);
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
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
expect_empty_group(const fs::path& path)
{
    EXPECT(fs::is_regular_file(path), "Expected ", path, " to exist");
    const auto json = nlohmann::json::parse(read_file(path));
    EXPECT_STR_EQ(json["node_type"].get<std::string>().c_str(), "group");
    EXPECT(json["attributes"].empty(), "Expected empty attributes in ", path);
}

void
expect_array()
{
    const fs::path array_meta =
      test_path / "path" / "to" / "data" / "zarr.json";
    const auto array_json = nlohmann::json::parse(read_file(array_meta));
    EXPECT_STR_EQ(array_json["node_type"].get<std::string>().c_str(), "array");
}

ZarrStream*
create_stream(ZarrIntermediateGroups mode)
{
    static const std::string store_path = test_path.string();
    ZarrArraySettings array = {
        .output_key = "path/to/data",
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint16,
    };
    ZarrStreamSettings settings = {
        .store_path = store_path.c_str(),
        .s3_settings = nullptr,
        .max_threads = 0,
        .arrays = &array,
        .array_count = 1,
        .intermediate_groups = mode,
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 3));

    ZarrDimensionProperties* dim = settings.arrays->dimensions;
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

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);

    return stream;
}

void
stream_frames(ZarrIntermediateGroups mode)
{
    // the guard frees the stream if a check below throws, so no writer thread
    // outlives the test and holds files open while they are removed
    StreamPtr stream(create_stream(mode), &ZarrStream_destroy);
    EXPECT(stream != nullptr, "Failed to create stream");

    const std::vector<uint16_t> frame(array_width * array_height, 1);
    size_t bytes_out;
    for (unsigned int i = 0; i < array_timepoints; ++i) {
        ZarrStatusCode status = ZarrStream_append(
          stream.get(), frame.data(), bytes_of_frame, &bytes_out, nullptr);
        EXPECT(status == ZarrStatusCode_Success,
               "Failed to append frame ",
               i,
               ": ",
               Zarr_get_status_message(status));
    }

    const ZarrStatusCode status = ZarrStream_close(stream.release());
    EXPECT(status == ZarrStatusCode_Success,
           "Failed to close stream: ",
           Zarr_get_status_message(status));
}

void
run_case(const char* name,
         ZarrIntermediateGroups mode,
         const std::function<void()>& prepare,
         const std::function<void()>& verify)
{
    LOG_INFO("Case: ", name);
    remove_test_path();
    prepare();
    stream_frames(mode);
    verify();
    expect_array();
}

void
write_caller_groups()
{
    write_file(test_path / "zarr.json", root_metadata);
    write_file(test_path / "path" / "zarr.json", path_metadata);
}

void
test_if_missing_keeps_caller_groups()
{
    run_case("if_missing keeps caller groups",
             ZarrIntermediateGroups_IfMissing,
             write_caller_groups,
             [] {
                 // the caller's groups are untouched, byte for byte
                 EXPECT_STR_EQ(read_file(test_path / "zarr.json").c_str(),
                               root_metadata.c_str());
                 EXPECT_STR_EQ(
                   read_file(test_path / "path" / "zarr.json").c_str(),
                   path_metadata.c_str());

                 // a missing intermediate group is still written, so the
                 // hierarchy stays navigable from the root
                 expect_empty_group(test_path / "path" / "to" / "zarr.json");
             });
}

void
test_if_missing_replaces_non_groups()
{
    run_case(
      "if_missing replaces non-group metadata",
      ZarrIntermediateGroups_IfMissing,
      [] {
          write_file(test_path / "zarr.json", truncated_metadata);
          write_file(test_path / "path" / "zarr.json", stale_array_metadata);
      },
      [] {
          expect_empty_group(test_path / "zarr.json");
          expect_empty_group(test_path / "path" / "zarr.json");
          expect_empty_group(test_path / "path" / "to" / "zarr.json");
      });
}

void
test_always_replaces_caller_groups()
{
    run_case("always replaces caller groups",
             ZarrIntermediateGroups_Always,
             write_caller_groups,
             [] {
                 expect_empty_group(test_path / "zarr.json");
                 expect_empty_group(test_path / "path" / "zarr.json");
                 expect_empty_group(test_path / "path" / "to" / "zarr.json");
             });
}

void
test_never_writes_no_groups()
{
    run_case(
      "never writes no groups",
      ZarrIntermediateGroups_Never,
      [] {},
      [] {
          for (const auto& path : { test_path / "zarr.json",
                                    test_path / "path" / "zarr.json",
                                    test_path / "path" / "to" / "zarr.json" }) {
              EXPECT(!fs::exists(path), "Expected ", path, " not to exist");
          }
      });
}

void
test_invalid_mode_is_rejected()
{
    LOG_INFO("Case: invalid mode is rejected");
    remove_test_path();
    StreamPtr stream(create_stream(ZarrIntermediateGroupsCount),
                     &ZarrStream_destroy);
    EXPECT(stream == nullptr, "Expected an invalid mode to be rejected");
}
} // namespace

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    int retval = 1;

    try {
        test_if_missing_keeps_caller_groups();
        test_if_missing_replaces_non_groups();
        test_always_replaces_caller_groups();
        test_never_writes_no_groups();
        test_invalid_mode_is_rejected();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    remove_test_path();

    return retval;
}
