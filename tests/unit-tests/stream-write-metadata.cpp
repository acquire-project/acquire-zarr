#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <nlohmann/json.hpp>

#include <istream>
#include <filesystem>

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = TEST ".zarr";
const std::string custom_metadata = R"({"foo":"bar"})";

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 200,
        .chunk_size_px = 20,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 300,
        .chunk_size_px = 30,
        .shard_size_chunks = 1,
    };
}

ZarrStream*
create_stream(const std::optional<std::string>& array_key = std::nullopt,
              bool multiscale = false)
{
    const std::string store_path = base_dir.string();

    ZarrStreamSettings settings;
    memset(&settings, 0, sizeof(settings));
    settings.store_path = store_path.c_str();

    CHECK(ZarrStatusCode_Success ==
          ZarrStreamSettings_create_arrays(&settings, 1));
    configure_stream_dimensions(settings.arrays);

    if (array_key.has_value()) {
        settings.arrays->output_key = array_key->c_str();
    }

    if (multiscale) {
        settings.arrays->multiscale = true;
        settings.arrays->downsampling_method = ZarrDownsamplingMethod_Mean;
    }

    auto* stream = ZarrStream_create(&settings);
    ZarrStreamSettings_destroy_arrays(&settings);

    return stream;
}

void
check_metadata(const std::optional<std::string>& array_key,
               const std::optional<std::string>& metadata_key)
{
    const auto array_path =
      base_dir / (array_key.has_value() ? *array_key : "");
    const auto metadata_path = array_path / "zarr.json";
    CHECK(fs::is_regular_file(metadata_path));

    nlohmann::json metadata;
    {
        std::ifstream metadata_file(metadata_path);
        CHECK(metadata_file.is_open());
        metadata_file >> metadata;

        EXPECT(metadata.contains("attributes"),
               "Missing 'attributes' in stream metadata");
        auto& container = metadata["attributes"];

        const std::string key = metadata_key.value_or("");
        if (!key.empty()) {
            EXPECT(container.contains(key),
                   "Missing '",
                   key,
                   "' in 'attributes' metadata.");
            container = container[key];
        }

        EXPECT(container.contains("foo"),
               "Missing ',",
               (key.empty() ? "" : key + "/"),
               "foo' in stream metadata attributes");
        EXPECT(container["foo"] == "bar",
               "Unexpected value for ',",
               (key.empty() ? "" : key + "/"),
               "foo' in stream metadata attributes: ",
               container["foo"]);
    }
}

bool
destroy_directory()
{
    std::error_code ec;
    if (fs::is_directory(base_dir) && !fs::remove_all(base_dir, ec)) {
        LOG_ERROR("Failed to remove store path: ", ec.message().c_str());
        return false;
    }

    return true;
}

bool
test_no_metadata()
{
    auto* stream = create_stream();
    if (!stream) {
        LOG_ERROR("Failed to create stream without metadata");
        return false;
    }

    bool retval = true;

    // immediately destroy the stream without writing anything
    ZarrStream_destroy(stream);
    if (fs::exists(base_dir / ".zarr.json")) {
        LOG_ERROR(
          "Metadata file should not exist when no metadata is written.");
        retval = false;
    }

    if (!destroy_directory()) {
        LOG_WARNING("Failed to destroy directory.");
    }

    return retval;
}

bool
test_with_metadata(const std::optional<std::string>& array_key,
                   const std::optional<std::string>& metadata_key,
                   bool multiscale)
{

    auto* stream = create_stream(array_key, multiscale);
    if (!stream) {
        LOG_ERROR("Failed to create stream.");
        return false;
    }

    const char* array_key_cstr = array_key ? array_key->c_str() : nullptr;
    const char* metadata_key_cstr =
      metadata_key ? metadata_key->c_str() : nullptr;

    // Write custom metadata to the stream
    CHECK(
      ZarrStatusCode_Success ==
      ZarrStream_write_custom_metadata(
        stream, array_key_cstr, metadata_key_cstr, custom_metadata.c_str()));

    ZarrStream_destroy(stream);
    check_metadata(array_key, metadata_key);

    CHECK(destroy_directory());

    return true;
}

bool
test_nonexistent_array_key()
{
    auto* stream = create_stream();
    if (!stream) {
        LOG_ERROR("Failed to create stream.");
        return false;
    }

    // Attempt to write custom metadata with a nonexistent array key
    const char* nonexistent_array_key = "nonexistent_array";
    CHECK(ZarrStatusCode_KeyNotFound ==
          ZarrStream_write_custom_metadata(stream,
                                           nonexistent_array_key,
                                           "metadata_key",
                                           custom_metadata.c_str()));

    ZarrStream_destroy(stream);
    return true;
}
} // namespace

int
main()
{
    int retval = 1;
    if (!destroy_directory()) {
        return retval;
    }

    try {
        // test that if we don't write any metadata, no metadata file is created
        // and no errors occur
        test_no_metadata();

        // test that writing custom metadata with an array key that doesn't
        // exist returns the expected error code
        test_nonexistent_array_key();

        // test various combinations of array key and metadata key
        // presence/absence
        test_with_metadata(std::nullopt, std::nullopt, false);
        test_with_metadata(std::nullopt, "", false);
        test_with_metadata(std::nullopt, "baz", false);
        test_with_metadata(std::nullopt, "baz/qux", false);
        test_with_metadata("test_array", std::nullopt, false);
        test_with_metadata("test_array", "", false);
        test_with_metadata("test_array", "baz", false);
        test_with_metadata("test_array", "baz/qux", false);

        // run the same tests with multiscale arrays, which have some different
        // metadata handling logic, to ensure that custom metadata is still
        // written and read correctly in that case
        test_with_metadata(std::nullopt, std::nullopt, true);
        test_with_metadata(std::nullopt, "", true);
        test_with_metadata(std::nullopt, "baz", true);
        test_with_metadata(std::nullopt, "baz/qux", true);
        test_with_metadata("test_array", std::nullopt, true);
        test_with_metadata("test_array", "", true);
        test_with_metadata("test_array", "baz", true);
        test_with_metadata("test_array", "baz/qux", true);

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    // cleanup
    if (!destroy_directory()) {
        retval = 1;
    }

    return retval;
}