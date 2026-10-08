#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>
#include "s3-test-helper.hh"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {
const unsigned int array_width = 32, array_height = 24, array_timepoints = 4;
const unsigned int chunk_width = 16, chunk_height = 16, chunk_timepoints = 2;

const size_t bytes_of_frame = array_width * array_height * sizeof(uint16_t);

const std::string store_path = TEST;
const std::string root_key = store_path + "/zarr.json";
const std::string path_key = store_path + "/path/zarr.json";
const std::string to_key = store_path + "/path/to/zarr.json";
const std::string array_key = store_path + "/path/to/data/zarr.json";

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
const std::string stale_array_metadata = R"({
    "zarr_format": 3,
    "node_type": "array",
    "attributes": {}
})";

using StreamPtr = std::unique_ptr<ZarrStream, decltype(&ZarrStream_destroy)>;

zarr::S3Settings s3;
std::unique_ptr<zarr::S3Client> client;

std::vector<std::string>
all_keys()
{
    std::vector<std::string> keys{ root_key, path_key, to_key, array_key };
    for (unsigned int t = 0; t < array_timepoints / chunk_timepoints; ++t) {
        for (unsigned int y = 0; y < 2; ++y) {
            for (unsigned int x = 0; x < 2; ++x) {
                keys.push_back(store_path + "/path/to/data/c/" +
                               std::to_string(t) + "/" + std::to_string(y) +
                               "/" + std::to_string(x));
            }
        }
    }
    return keys;
}

void
clear_store()
{
    // a missing key is not an error for DeleteObject
    remove_items(*client, s3.bucket_name, all_keys());
}

void
put(const std::string& key, const std::string& contents)
{
    const std::vector<uint8_t> bytes(contents.begin(), contents.end());
    EXPECT(
      client->put_object(s3.bucket_name, key, bytes), "Failed to put ", key);
}

std::string
get(const std::string& key)
{
    return get_object_contents(*client, s3.bucket_name, key);
}

void
expect_empty_group(const std::string& key)
{
    EXPECT(object_exists(*client, s3.bucket_name, key),
           "Expected ",
           key,
           " to exist");
    const auto json = nlohmann::json::parse(get(key));
    EXPECT_STR_EQ(json["node_type"].get<std::string>().c_str(), "group");
    EXPECT(json["attributes"].empty(), "Expected empty attributes in ", key);
}

ZarrStream*
create_stream(ZarrIntermediateGroups mode, bool overwrite)
{
    ZarrArraySettings array = {
        .output_key = "path/to/data",
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint16,
    };
    ZarrS3Settings s3_settings{
        .endpoint = s3.endpoint.c_str(),
        .bucket_name = s3.bucket_name.c_str(),
    };
    if (s3.region) {
        s3_settings.region = s3.region->c_str();
    }
    ZarrStreamSettings settings = {
        .store_path = store_path.c_str(),
        .s3_settings = &s3_settings,
        .max_threads = 0,
        .overwrite = overwrite,
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
stream_frames(ZarrIntermediateGroups mode, bool overwrite)
{
    StreamPtr stream(create_stream(mode, overwrite), &ZarrStream_destroy);
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
         bool overwrite,
         const std::function<void()>& prepare,
         const std::function<void()>& verify)
{
    LOG_INFO("Case: ", name);
    clear_store();
    prepare();
    stream_frames(mode, overwrite);
    verify();
    EXPECT(object_exists(*client, s3.bucket_name, array_key),
           "Expected ",
           array_key,
           " to exist");
}

void
write_caller_groups()
{
    put(root_key, root_metadata);
    put(path_key, path_metadata);
}
} // namespace

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    const auto settings = test::s3_settings_from_env();
    if (!settings) {
        LOG_WARNING("S3 not configured. Skipping test.");
        return 0;
    }
    s3 = *settings;

    int retval = 1;

    try {
        client = std::make_unique<zarr::S3Client>(s3);

        run_case(
          "if_missing keeps caller groups",
          ZarrIntermediateGroups_IfMissing,
          false,
          write_caller_groups,
          [] {
              EXPECT_STR_EQ(get(root_key).c_str(), root_metadata.c_str());
              EXPECT_STR_EQ(get(path_key).c_str(), path_metadata.c_str());
              expect_empty_group(to_key);
          });

        run_case(
          "if_missing replaces non-group metadata",
          ZarrIntermediateGroups_IfMissing,
          false,
          [] { put(path_key, stale_array_metadata); },
          [] {
              expect_empty_group(root_key);
              expect_empty_group(path_key);
              expect_empty_group(to_key);
          });

        // S3 is not cleared by overwrite, so the stream must not keep what an
        // earlier run left there
        run_case("if_missing with overwrite replaces groups",
                 ZarrIntermediateGroups_IfMissing,
                 true,
                 write_caller_groups,
                 [] {
                     expect_empty_group(root_key);
                     expect_empty_group(path_key);
                     expect_empty_group(to_key);
                 });

        run_case("always replaces caller groups",
                 ZarrIntermediateGroups_Always,
                 false,
                 write_caller_groups,
                 [] {
                     expect_empty_group(root_key);
                     expect_empty_group(path_key);
                     expect_empty_group(to_key);
                 });

        run_case(
          "never writes no groups",
          ZarrIntermediateGroups_Never,
          false,
          [] {},
          [] {
              for (const auto& key : { root_key, path_key, to_key }) {
                  EXPECT(!object_exists(*client, s3.bucket_name, key),
                         "Expected ",
                         key,
                         " not to exist");
              }
          });

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    if (client) {
        clear_store();
        client.reset();
    }

    return retval;
}
