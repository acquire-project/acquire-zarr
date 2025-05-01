#include "unit.test.macros.hh"
#include "zarr.stream.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace {
const std::string store_path = TEST ".zarr";

ZarrStream*
configure_stream()
{
    ZarrStreamSettings settings{
        .store_path = store_path.c_str(),
        .s3_settings = nullptr,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
        .multiscale = false,
        .data_type = ZarrDataType_uint8,
        .version = ZarrVersion_3,
        .max_threads = 0,
    };

    settings.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    settings.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    settings.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    auto* stream = new ZarrStream(&settings);
    delete[] settings.dimensions;

    return stream;
}

bool
configure_new_group()
{
    bool retval = false;

    ZarrStream* stream = configure_stream();

    ZarrGroupProperties properties = {
        .store_key = "new_group",
        .data_type = ZarrDataType_uint16,
        .multiscale = false,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
    };

    properties.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 0,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    properties.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    properties.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    try {
        CHECK(stream->configure_group(&properties) == ZarrStatusCode_Success);
        finalize_stream(stream);

        fs::path new_group_metadata_path =
          fs::path(store_path) / "new_group" / "zarr.json";
        EXPECT(fs::is_regular_file(new_group_metadata_path),
               "Expected metadata file ",
               new_group_metadata_path.string(),
               " does not exist");

        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    delete[] properties.dimensions;
    delete stream;
    return retval;
}

bool
configure_nested_group()
{
    bool retval = false;

    ZarrStream* stream = configure_stream();

    ZarrGroupProperties properties = {
        .store_key = "A/1",
        .data_type = ZarrDataType_uint16,
        .multiscale = false,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
    };

    properties.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 0,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    properties.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    properties.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    try {
        CHECK(stream->configure_group(&properties) == ZarrStatusCode_Success);
        finalize_stream(stream);

        fs::path new_group_metadata_path =
          fs::path(store_path) / "A" / "1" / "zarr.json";
        EXPECT(fs::is_regular_file(new_group_metadata_path),
               "Expected metadata file ",
               new_group_metadata_path.string(),
               " does not exist");

        // as a terminal group, it should have OME metadata
        std::ifstream f(new_group_metadata_path);
        auto metadata = nlohmann::json::parse(f);
        CHECK(metadata["attributes"].contains("ome"));

        // intermediate group needs metadata too
        fs::path intermediate_group_metadata_path =
          fs::path(store_path) / "A" / "zarr.json";
        EXPECT(fs::is_regular_file(intermediate_group_metadata_path),
               "Expected metadata file ",
               intermediate_group_metadata_path.string(),
               " does not exist.");

        // as an intermediate group, it should not have OME metadata
        f = std::ifstream(intermediate_group_metadata_path);
        metadata = nlohmann::json::parse(f);
        CHECK(!metadata["attributes"].contains("ome"));

        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    delete[] properties.dimensions;
    delete stream;
    return retval;
}

bool
configure_nested_group_but_not_intermediate_array()
{
    bool retval = false;

    ZarrStream* stream = configure_stream();

    ZarrGroupProperties group_props = {
        .store_key = "A/1",
        .data_type = ZarrDataType_uint16,
        .multiscale = false,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
    };

    group_props.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 0,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    group_props.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    group_props.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    ZarrArrayProperties array_props = {
        .store_key = "A", // this should fail because group "A" already exists
        .data_type = ZarrDataType_uint16,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[3],
        .dimension_count = 3,
    };

    array_props.dimensions[0] = {
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = 0,
        .chunk_size_px = 64,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    array_props.dimensions[1] = {
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    array_props.dimensions[2] = {
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 32,
        .shard_size_chunks = 1,
        .unit = "um",
        .scale = 1.0,
    };

    try {
        CHECK(stream->configure_group(&group_props) == ZarrStatusCode_Success);
        CHECK(stream->configure_array(&array_props) != ZarrStatusCode_Success);
        finalize_stream(stream);

        fs::path new_group_metadata_path =
          fs::path(store_path) / "A" / "1" / "zarr.json";
        EXPECT(fs::is_regular_file(new_group_metadata_path),
               "Expected metadata file ",
               new_group_metadata_path.string(),
               " does not exist");

        // as a terminal group, it should have OME metadata
        std::ifstream f(new_group_metadata_path);
        auto metadata = nlohmann::json::parse(f);
        CHECK(metadata["attributes"].contains("ome"));

        // intermediate group needs metadata too
        fs::path intermediate_group_metadata_path =
          fs::path(store_path) / "A" / "zarr.json";
        EXPECT(fs::is_regular_file(intermediate_group_metadata_path),
               "Expected metadata file ",
               intermediate_group_metadata_path.string(),
               " does not exist.");

        // as an intermediate group, it should not have OME metadata
        f = std::ifstream(intermediate_group_metadata_path);
        metadata = nlohmann::json::parse(f);
        CHECK(!metadata["attributes"].contains("ome"));

        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    delete[] group_props.dimensions;
    delete[] array_props.dimensions;
    delete stream;
    return retval;
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        CHECK(configure_new_group());
        CHECK(configure_nested_group());
        CHECK(configure_nested_group_but_not_intermediate_array());

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Error: ", e.what());
    }

    // cleanup
    if (fs::is_directory(store_path)) {
        fs::remove_all(store_path);
    }

    return retval;
}