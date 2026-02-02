#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>
#include <miniocpp/client.h>

#include <filesystem>
#include <fstream>
#include <vector>

#ifdef GetObject
#undef GetObject
#endif

namespace fs = std::filesystem;

namespace {
const std::string store_path = TEST ".zarr";
const std::string output_key = "array";
const auto test_path_fs = (fs::temp_directory_path() / store_path).string();

std::string s3_endpoint, s3_bucket_name, s3_access_key_id, s3_secret_access_key,
            s3_region;
ZarrS3Settings s3_settings{};

constexpr unsigned int array_width = 64, array_height = 48, array_planes = 2;
constexpr unsigned int chunk_width = 64, chunk_height = 48, chunk_planes = 2;
constexpr unsigned int shard_width = 1, shard_height = 1, shard_planes = 1;

std::vector expected_paths{
    store_path + "/zarr.json",
    store_path + "/" + output_key + "/zarr.json",
    store_path + "/" + output_key + "/c/0/0/0"
};

constexpr ZarrDataType dtype = ZarrDataType_uint16;
constexpr size_t npx_frame = array_width * array_height;
const std::vector<uint16_t> frame_data(npx_frame, 1);
constexpr size_t bytes_of_frame = npx_frame * sizeof(frame_data[0]);

constexpr uint32_t frames_to_acquire = array_planes;
constexpr size_t expected_shard_size = bytes_of_frame * chunk_planes + // data
                                       2 * sizeof(uint64_t) + // table
                                       sizeof(uint32_t); // checksum

bool
s3_get_credentials()
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        LOG_WARNING("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    s3_endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_WARNING("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    s3_bucket_name = env;

    if (!(env = std::getenv("AWS_ACCESS_KEY_ID"))) {
        LOG_WARNING("AWS_ACCESS_KEY_ID not set.");
        return false;
    }
    s3_access_key_id = env;

    if (!(env = std::getenv("AWS_SECRET_ACCESS_KEY"))) {
        LOG_WARNING("AWS_SECRET_ACCESS_KEY not set.");
        return false;
    }
    s3_secret_access_key = env;

    env = std::getenv("ZARR_S3_REGION");
    if (env) {
        s3_region = env;
    }

    return true;
}

bool
fs_file_exists(const std::string& object_name)
{
    return fs::is_regular_file(object_name);
}

size_t
fs_get_file_size(const std::string& object_name)
{
    return fs::file_size(object_name);
}

std::string
fs_get_object_contents_as_string(const std::string& object_name)
{
    std::stringstream ss;
    const std::ifstream f(object_name);
    ss << f.rdbuf();

    return ss.str();
}

std::vector<uint8_t>
fs_get_object_contents_as_bytes(const std::string& object_name)
{
    std::ifstream f(object_name, std::ios::binary);
    // Get file size
    f.seekg(0, std::ios::end);
    const auto file_size = f.tellg();
    f.seekg(0, std::ios::beg);

    std::vector<uint8_t> bytes_out(file_size);
    f.read(reinterpret_cast<char*>(bytes_out.data()), file_size);

    CHECK(f.good());

    return bytes_out;
}

bool
s3_object_exists(const std::string& object_name, minio::s3::Client& client)
{
    minio::s3::StatObjectArgs args;
    args.bucket = s3_bucket_name;
    args.object = object_name;

    const minio::s3::StatObjectResponse response = client.StatObject(args);

    return static_cast<bool>(response);
}

size_t
s3_get_object_size(const std::string& object_name, minio::s3::Client& client)
{
    minio::s3::StatObjectArgs args;
    args.bucket = s3_bucket_name;
    args.object = object_name;

    const minio::s3::StatObjectResponse response = client.StatObject(args);

    if (!response) {
        LOG_ERROR("Failed to get object size: ", object_name);
        return 0;
    }

    return response.size;
}

std::string
s3_get_object_contents_as_string(const std::string& object_name,
                                 minio::s3::Client& client)
{
    std::stringstream ss;

    minio::s3::GetObjectArgs go_args;
    go_args.bucket = s3_bucket_name;
    go_args.object = object_name;
    go_args.datafunc = [&ss
        ](const minio::http::DataFunctionArgs& args) -> bool {
            ss << args.datachunk;
            return true;
        };

    // Call get object.
    minio::s3::GetObjectResponse resp = client.GetObject(go_args);

    return ss.str();
}

std::vector<uint8_t>
s3_get_object_contents_as_bytes(const std::string& object_name,
                                minio::s3::Client& client)
{
    std::vector<uint8_t> data;

    minio::s3::GetObjectArgs go_args;
    go_args.bucket = s3_bucket_name;
    go_args.object = object_name;
    go_args.datafunc = [&data](
        const minio::http::DataFunctionArgs& args) -> bool {
            const auto* chunk_data = reinterpret_cast<const uint8_t*>(args.
                datachunk.data());
            data.insert(data.end(),
                        chunk_data,
                        chunk_data + args.datachunk.size());
            return true;
        };

    minio::s3::GetObjectResponse resp = client.GetObject(go_args);

    return data;
}

bool
s3_remove_items(const std::vector<std::string>& item_keys,
                minio::s3::Client& client)
{
    std::list<minio::s3::DeleteObject> objects;
    for (const auto& key : item_keys) {
        minio::s3::DeleteObject object;
        object.name = key;
        objects.push_back(object);
    }

    minio::s3::RemoveObjectsArgs args;
    args.bucket = s3_bucket_name;

    auto it = objects.begin();

    args.func = [&objects = objects,
            &i = it](minio::s3::DeleteObject& obj) -> bool {
            if (i == objects.end())
                return false;
            obj = *i;
            i++;
            return true;
        };

    minio::s3::RemoveObjectsResult result = client.RemoveObjects(args);
    for (; result; result++) {
        minio::s3::DeleteError err = *result;
        if (!err) {
            LOG_ERROR("Failed to delete object ",
                      err.object_name,
                      ": ",
                      err.message);
            return false;
        }
    }

    return true;
}

void
setup_stream_array(ZarrStreamSettings& settings)
{
    CHECK(!settings.arrays);
    CHECK(settings.array_count == 0);

    CHECK_OK(ZarrStreamSettings_create_arrays(&settings, 1));
    ZarrArraySettings* const array = settings.arrays;

    CHECK_OK(ZarrArraySettings_create_dimension_array(array, 3));
    ZarrDimensionProperties* dim = array->dimensions;

    *dim++ = DIM("z",
                 ZarrDimensionType_Space,
                 array_planes,
                 chunk_planes,
                 shard_planes,
                 "millimeter",
                 1.4);
    *dim++ = DIM("y",
                 ZarrDimensionType_Space,
                 array_height,
                 chunk_height,
                 shard_height,
                 "micrometer",
                 0.9);
    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               chunk_width,
               shard_width,
               "micrometer",
               0.9);

    array->output_key = output_key.c_str();
    array->data_type = dtype;
}

ZarrStream*
setup_fs(ZarrStreamSettings& settings)
{
    memset(&settings, 0, sizeof(settings));
    settings.store_path = test_path_fs.c_str();

    // setup test array
    setup_stream_array(settings);

    return ZarrStream_create(&settings);
}

ZarrStream*
setup_s3(ZarrStreamSettings& settings)
{
    memset(&settings, 0, sizeof(settings));
    settings.store_path = store_path.c_str();

    if (!s3_get_credentials()) {
        LOG_WARNING("Failed to get S3 credentials. Skipping S3 test.");
        return nullptr;
    }

    // setup test array
    setup_stream_array(settings);

    // configure S3
    s3_settings.endpoint = s3_endpoint.c_str();
    s3_settings.bucket_name = s3_bucket_name.c_str();
    if (!s3_region.empty()) {
        s3_settings.region = s3_region.c_str();
    }
    settings.s3_settings = &s3_settings;

    return ZarrStream_create(&settings);
}

size_t
do_stream(ZarrStream* stream)
{
    size_t count = 0;

    const void* data = frame_data.data();
    try {
        size_t bytes_out;
        // append nullptr, i.e., "stream" an empty frame
        if (ZarrStream_append(stream,
                              nullptr,
                              bytes_of_frame,
                              &bytes_out,
                              output_key.c_str());
            bytes_out != bytes_of_frame) {
            LOG_ERROR("Failed to append nullptr. Bytes in: ",
                      bytes_of_frame,
                      ", bytes out: ",
                      bytes_out);

            return 0;
        }
        ++count;

        for (auto i = 1; i < frames_to_acquire; ++i) {
            if (ZarrStream_append(stream,
                                  data,
                                  bytes_of_frame,
                                  &bytes_out,
                                  output_key.c_str());
                bytes_out != bytes_of_frame) {
                LOG_ERROR("Failed to append frame. ",
                          i,
                          " of ",
                          frames_to_acquire,
                          ". Bytes in: ",
                          bytes_of_frame,
                          ", bytes out: ",
                          bytes_out);
                break;
            }
            ++count;
        }
    } catch (const std::exception& exc) {
        LOG_ERROR("Error streaming: ", exc.what());
    }

    return count;
}

bool
verify_fs(const ZarrStreamSettings& settings)
{
    const auto array_path = fs::path(test_path_fs) / output_key;
    const auto data_file_path = (array_path / "c" / "0" / "0" / "0").string();

    // should have flushed
    if (!fs_file_exists(data_file_path)) {
        LOG_ERROR("Data file path ",
                  data_file_path,
                  " does not exist or is not a file.");
        return false;
    }

    // should be the right size
    if (size_t object_size = fs_get_file_size(data_file_path);
        object_size != expected_shard_size) {
        LOG_ERROR("Expected file size of ",
                  expected_shard_size,
                  ", got ",
                  object_size);
        return false;
    }

    // should have the correct contents
    const auto data = fs_get_object_contents_as_bytes(data_file_path);
    const std::span data_u16 = { reinterpret_cast<const uint16_t*>(data.data()),
                                 2 * npx_frame };
    for (auto i = 0; i < npx_frame; ++i) {
        if (data_u16[i] != 0) {
            LOG_ERROR("Expected data at index ",
                      i,
                      " to be zero, got ",
                      data_u16[i]);
            return false;
        }
    }
    for (auto i = npx_frame; i < 2 * npx_frame; ++i) {
        if (data_u16[i] != frame_data[i - npx_frame]) {
            LOG_ERROR("Expected data at index ",
                      i,
                      " to be ",
                      frame_data[i - npx_frame],
                      ", got ",
                      data_u16[i]);
            return false;
        }
    }

    return true;

    return true;
}

bool
verify_s3(const ZarrStreamSettings& settings, minio::s3::Client& client)
{
    const std::string array_path = store_path + "/" + output_key;
    const auto data_file_path = array_path + "/c/0/0/0";

    // should have flushed
    if (!s3_object_exists(data_file_path, client)) {
        LOG_ERROR("Data file path ",
                  data_file_path,
                  " does not exist or is not an object.");
        return false;
    }

    // should be the right size
    if (size_t object_size = s3_get_object_size(data_file_path, client);
        object_size != expected_shard_size) {
        LOG_ERROR("Expected object size of ",
                  expected_shard_size,
                  ", got ",
                  object_size);
        return false;
    }

    // should have the correct contents
    const auto data = s3_get_object_contents_as_bytes(data_file_path, client);
    const std::span data_u16 = { reinterpret_cast<const uint16_t*>(data.data()),
                                 2 * npx_frame };
    for (auto i = 0; i < npx_frame; ++i) {
        if (data_u16[i] != 0) {
            LOG_ERROR("Expected data at index ",
                      i,
                      " to be zero, got ",
                      data_u16[i]);
            return false;
        }
    }
    for (auto i = npx_frame; i < 2 * npx_frame; ++i) {
        if (data_u16[i] != frame_data[i - npx_frame]) {
            LOG_ERROR("Expected data at index ",
                      i,
                      " to be ",
                      frame_data[i - npx_frame],
                      ", got ",
                      data_u16[i]);
            return false;
        }
    }

    return true;
}

void
teardown_stream_array(const ZarrStreamSettings& settings)
{
    if (settings.arrays) {
        ZarrArraySettings_destroy_dimension_array(settings.arrays);
    }
}

void
teardown_fs(const ZarrStreamSettings& settings)
{
    teardown_stream_array(settings);
    if (std::error_code ec; !fs::remove_all(test_path_fs, ec)) {
        LOG_WARNING("Failed to remove ", test_path_fs, ": ", ec.message());
    }
}

void
teardown_s3(const ZarrStreamSettings& settings, minio::s3::Client& client)
{
    teardown_stream_array(settings);
    s3_remove_items(expected_paths, client);
}
} // namespace

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);
    // test S3
    {
        ZarrStreamSettings settings{};
        if (ZarrStream* stream = setup_s3(settings); stream != nullptr) {
            minio::s3::BaseUrl url(s3_endpoint);
            url.https = s3_endpoint.starts_with("https://");

            minio::creds::StaticProvider provider(s3_access_key_id,
                                                  s3_secret_access_key);
            minio::s3::Client client(url, &provider);

            if (const size_t frames_out = do_stream(stream);
                frames_out == frames_to_acquire) {
                ZarrStream_destroy(stream);

                if (!verify_s3(settings, client)) {
                    teardown_s3(settings, client);
                    return 1;
                }
                teardown_s3(settings, client);
            } else {
                LOG_ERROR("Actual frames streamed ",
                          frames_out,
                          " does not match expected frames streamed ",
                          frames_to_acquire);
                ZarrStream_destroy(stream);
                teardown_s3(settings, client);
                return 1;
            }
        }
    }

    // test filesystem
    {
        ZarrStreamSettings settings{};
        if (ZarrStream* stream = setup_fs(settings); stream != nullptr) {
            if (const size_t frames_out = do_stream(stream);
                frames_out == frames_to_acquire) {
                ZarrStream_destroy(stream);

                if (!verify_fs(settings)) {
                    teardown_fs(settings);
                    return 1;
                }
                teardown_fs(settings);
            } else {
                LOG_ERROR("Actual frames streamed ",
                          frames_out,
                          " does not match expected frames streamed ",
                          frames_to_acquire);
                ZarrStream_destroy(stream);
                teardown_fs(settings);
                return 1;
            }
        }
    }

    return 0;
}