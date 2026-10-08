#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
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
} // namespace

ZarrStream*
setup()
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
verify()
{
    // the caller's groups are untouched, byte for byte
    EXPECT_STR_EQ(read_file(test_path / "zarr.json").c_str(),
                  root_metadata.c_str());
    EXPECT_STR_EQ(read_file(test_path / "path" / "zarr.json").c_str(),
                  path_metadata.c_str());

    // a missing intermediate group is still written, so the hierarchy stays
    // navigable from the root
    const fs::path to_meta = test_path / "path" / "to" / "zarr.json";
    EXPECT(fs::is_regular_file(to_meta), "Expected ", to_meta, " to exist");
    const auto to_json = nlohmann::json::parse(read_file(to_meta));
    EXPECT_STR_EQ(to_json["node_type"].get<std::string>().c_str(), "group");
    EXPECT(to_json["attributes"].empty(), "Expected empty attributes");

    const fs::path array_meta =
      test_path / "path" / "to" / "data" / "zarr.json";
    const auto array_json = nlohmann::json::parse(read_file(array_meta));
    EXPECT_STR_EQ(array_json["node_type"].get<std::string>().c_str(), "array");
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }
    write_file(test_path / "zarr.json", root_metadata);
    write_file(test_path / "path" / "zarr.json", path_metadata);

    int retval = 1;

    try {
        auto* stream = setup();
        EXPECT(stream != nullptr, "Failed to create stream");

        const std::vector<uint16_t> frame(array_width * array_height, 1);
        size_t bytes_out;
        for (auto i = 0; i < array_timepoints; ++i) {
            ZarrStatusCode status = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
        }

        ZarrStream_destroy(stream);

        verify();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    return retval;
}
