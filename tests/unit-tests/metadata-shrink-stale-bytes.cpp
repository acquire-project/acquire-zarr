// Reproduces the "shrinking metadata leaves stale trailing bytes" eventuality.
//
// Metadata is written at offset 0 through the file sink, and neither backend
// truncates (Win32 OPEN_ALWAYS; POSIX O_WRONLY|O_CREAT, no O_TRUNC/ftruncate).
// So if a zarr.json is rewritten *shorter* than a previous version, the tail of
// the old document survives past the end of the new one. Custom metadata is the
// way to make the document shrink: write a large blob under `attributes`, then
// rewrite with a small (or empty) one.
//
// This drives that through the public API via two acquisitions to the same
// store (overwrite=false keeps the first store's zarr.json), then reads the
// result back two ways:
//   - ifstream >> json : the codebase's own reader (lenient: stops at the first
//                        complete value, so it ignores a stale tail)
//   - json::parse(str) : strict, like zarr-python / napari (json.loads), which
//                        rejects trailing tokens
//
// The test asserts the strict-parse property holds. If the bug is present it
// fails here, which is the signal we're after.

#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;

namespace {
const fs::path base_dir = TEST ".zarr";
const fs::path root_metadata = base_dir / "zarr.json";

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));

    settings->dimensions[0] = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
    };
    settings->dimensions[1] = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 200,
        .chunk_size_px = 20,
        .shard_size_chunks = 1,
    };
    settings->dimensions[2] = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 300,
        .chunk_size_px = 30,
        .shard_size_chunks = 1,
    };
}

// Create a stream over base_dir. overwrite=false so a second acquisition does
// not wipe the first one's zarr.json.
ZarrStream*
create_stream()
{
    const std::string store_path = base_dir.string();

    ZarrStreamSettings settings;
    memset(&settings, 0, sizeof(settings));
    settings.store_path = store_path.c_str();
    settings.overwrite = false;

    CHECK(ZarrStatusCode_Success ==
          ZarrStreamSettings_create_arrays(&settings, 1));
    configure_stream_dimensions(settings.arrays);

    auto* stream = ZarrStream_create(&settings);
    ZarrStreamSettings_destroy_arrays(&settings);

    return stream;
}

// Write custom metadata to the root zarr.json under `attributes` and close.
void
write_root_metadata_and_close(const std::string& metadata_key,
                              const std::string& metadata_json)
{
    auto* stream = create_stream();
    CHECK(stream);

    CHECK(ZarrStatusCode_Success ==
          ZarrStream_write_custom_metadata(
            stream, nullptr, metadata_key.c_str(), metadata_json.c_str()));

    ZarrStream_destroy(stream);
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

void
test_shrinking_metadata_leaves_stale_bytes()
{
    CHECK(destroy_directory());

    // 1) First acquisition: a large custom-metadata blob -> large zarr.json.
    const std::string big_blob(8192, 'x');
    nlohmann::json big = { { "payload", big_blob } };
    write_root_metadata_and_close("big", big.dump());

    CHECK(fs::is_regular_file(root_metadata));
    const auto size_after_big = fs::file_size(root_metadata);
    LOG_INFO("zarr.json size after large metadata: ", size_after_big);

    // 2) Second acquisition to the same store (overwrite=false): a tiny blob.
    //    This rewrites zarr.json shorter, at offset 0, with no truncation.
    write_root_metadata_and_close("small", R"("ok")");

    const auto size_after_small = fs::file_size(root_metadata);
    LOG_INFO("zarr.json size after small metadata: ", size_after_small);

    // Read the raw file once so we can try both parse strategies on it.
    std::string contents;
    {
        std::ifstream f(root_metadata, std::ios::binary);
        CHECK(f.is_open());
        std::ostringstream ss;
        ss << f.rdbuf();
        contents = ss.str();
    }

    // (a) The codebase's own reader: lenient, stops at the first complete value.
    bool lenient_ok = true;
    try {
        std::istringstream is(contents);
        nlohmann::json j;
        is >> j;
    } catch (const std::exception& exc) {
        lenient_ok = false;
        LOG_INFO("lenient (operator>>) parse threw: ", exc.what());
    }
    LOG_INFO("lenient (operator>>) parse ok: ", lenient_ok);

    // (b) Strict parse, like zarr-python / napari (json.loads): rejects a tail.
    bool strict_ok = true;
    std::string strict_err;
    try {
        auto j = nlohmann::json::parse(contents);
    } catch (const std::exception& exc) {
        strict_ok = false;
        strict_err = exc.what();
        LOG_INFO("strict (json::parse) parse threw: ", exc.what());
    }

    // The property we want: a freshly written zarr.json is always strictly
    // valid. If the file still carries the old document's tail, this fails --
    // which is the eventuality we're probing for.
    EXPECT(strict_ok,
           "Strict parse of zarr.json failed; stale trailing bytes from the "
           "previous (larger) metadata likely survived: ",
           strict_err);

    CHECK(destroy_directory());
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
        test_shrinking_metadata_leaves_stale_bytes();
        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    if (!destroy_directory()) {
        retval = 1;
    }

    return retval;
}