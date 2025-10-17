#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <fstream>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();
} // namespace

ZarrStream*
setup()
{
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .max_threads = 0, // use all available threads
        .overwrite = true,
    };
    ZarrDimensionProperties* dim;

    CHECK_OK(ZarrStreamSettings_create_arrays(&settings, 3));

    // create the labels array
    settings.arrays[0] = {
        .output_key = "labels",
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint16,
    };

    // configure labels array dimensions
    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 5));
    dim = settings.arrays[0].dimensions;
    *dim = DIM("t", ZarrDimensionType_Time, 0, 3, 1, nullptr, 1.0);

    dim = settings.arrays[0].dimensions + 1;
    *dim = DIM("c", ZarrDimensionType_Channel, 3, 1, 3, nullptr, 1.0);

    dim = settings.arrays[0].dimensions + 2;
    *dim = DIM("z", ZarrDimensionType_Space, 4, 2, 2, "millimeter", 1.4);

    dim = settings.arrays[0].dimensions + 3;
    *dim = DIM("y", ZarrDimensionType_Space, 48, 16, 3, "micrometer", 0.9);

    dim = settings.arrays[0].dimensions + 4;
    *dim = DIM("x", ZarrDimensionType_Space, 64, 16, 2, "micrometer", 0.9);

    // create the first array
    settings.arrays[1] = {
        .output_key = "path/to/array1",
        .compression_settings = nullptr,
        .data_type = ZarrDataType_uint8,
        .multiscale = true,
    };

    // configure first array dimensions
    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays + 1, 4));
    dim = settings.arrays[1].dimensions;
    *dim = DIM("t", ZarrDimensionType_Time, 0, 5, 1, nullptr, 1.0);

    dim = settings.arrays[1].dimensions + 1;
    *dim = DIM("z", ZarrDimensionType_Space, 6, 3, 2, "millimeter", 1.0);

    dim = settings.arrays[1].dimensions + 2;
    *dim = DIM("y", ZarrDimensionType_Space, 48, 16, 1, "micrometer", 1.0);

    dim = settings.arrays[1].dimensions + 3;
    *dim = DIM("x", ZarrDimensionType_Space, 64, 16, 1, "micrometer", 1.0);

    // create the second array
    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 2,
        .shuffle = 2,
    };

    settings.arrays[2] = {
        .output_key = "path/to/array2",
        .compression_settings = &compression_settings,
        .data_type = ZarrDataType_uint32,
    };

    // configure second array dimensions
    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays + 2, 3));
    dim = settings.arrays[2].dimensions;
    *dim = DIM("z", ZarrDimensionType_Space, 0, 3, 1, nullptr, 1.0);

    dim = settings.arrays[2].dimensions + 1;
    *dim = DIM("y", ZarrDimensionType_Space, 48, 16, 1, "micrometer", 1.0);

    dim = settings.arrays[2].dimensions + 2;
    *dim = DIM("x", ZarrDimensionType_Space, 64, 16, 1, "micrometer", 1.0);

    auto* stream = ZarrStream_create(&settings);
    ZarrStreamSettings_destroy_arrays(&settings);

    return stream;
}

void
verify_intermediate_group_metadata(const nlohmann::json& meta)
{
    /*
     * Metadata looks like this:
     * {
     *  "attributes": {},
     *  "consolidated_metadata": null,
     *  "node_type": "group",
     *  "zarr_format": 3
     * }
     */
    EXPECT(meta.is_object(), "Expected metadata to be an object");

    EXPECT(meta.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = meta["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(meta.contains("node_type"), "Expected key 'node_type' in metadata");
    auto node_type = meta["node_type"].get<std::string>();
    EXPECT(node_type == "group",
           "Expected node_type to be 'group', got '",
           node_type,
           "'");

    EXPECT(meta.contains("consolidated_metadata"),
           "Expected key 'consolidated_metadata' in metadata");
    EXPECT(meta["consolidated_metadata"].is_null(),
           "Expected consolidated_metadata to be null");

    EXPECT(meta.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(meta["attributes"].is_object(),
           "Expected attributes to be an object");
    EXPECT(meta["attributes"].empty(), "Expected attributes to be empty");
}

void
verify_shape(const nlohmann::json& metadata,
             const std::vector<int>& expected_shape)
{
    EXPECT(metadata.contains("shape"), "Expected key 'shape' in metadata");
    const auto& shape = metadata["shape"];
    EXPECT(shape.is_array(), "Expected shape to be an array");
    EXPECT_EQ(size_t, shape.size(), expected_shape.size());

    for (size_t i = 0; i < expected_shape.size(); ++i) {
        EXPECT_EQ(int, shape[i].get<int>(), expected_shape[i]);
    }
}

void
verify_dimension_names(const nlohmann::json& metadata,
                       const std::vector<std::string>& expected_dimension_names)
{
    EXPECT(metadata.contains("dimension_names"),
           "Expected key 'dimension_names' in metadata");
    const auto& dimension_names = metadata["dimension_names"];
    EXPECT(dimension_names.is_array(),
           "Expected dimension_names to be an array");
    EXPECT_EQ(size_t, dimension_names.size(), expected_dimension_names.size());

    for (size_t i = 0; i < expected_dimension_names.size(); ++i) {
        EXPECT(dimension_names[i].get<std::string>() ==
                 expected_dimension_names[i],
               "Expected dimension name at index ",
               i,
               " to be '",
               expected_dimension_names[i],
               "', got '",
               dimension_names[i].get<std::string>(),
               "'");
    }
}

void
verify_chunk_grid(const nlohmann::json& metadata,
                  const std::vector<int>& expected_chunk_shape)
{
    EXPECT(metadata.contains("chunk_grid"),
           "Expected key 'chunk_grid' in metadata");
    const auto& chunk_grid = metadata["chunk_grid"];
    EXPECT(chunk_grid.is_object(), "Expected chunk_grid to be an object");

    EXPECT(chunk_grid.contains("name"), "Expected key 'name' in chunk_grid");
    auto chunk_grid_name = chunk_grid["name"].get<std::string>();
    EXPECT(chunk_grid_name == "regular",
           "Expected chunk_grid name to be 'regular', got '",
           chunk_grid_name,
           "'");

    EXPECT(chunk_grid.contains("configuration"),
           "Expected key 'configuration' in chunk_grid");
    const auto& chunk_grid_config = chunk_grid["configuration"];
    EXPECT(chunk_grid_config.is_object(),
           "Expected chunk_grid configuration to be an object");

    EXPECT(chunk_grid_config.contains("chunk_shape"),
           "Expected key 'chunk_shape' in chunk_grid configuration");
    const auto& chunk_shape = chunk_grid_config["chunk_shape"];
    EXPECT(chunk_shape.is_array(), "Expected chunk_shape to be an array");
    EXPECT_EQ(size_t, chunk_shape.size(), expected_chunk_shape.size());

    for (size_t i = 0; i < expected_chunk_shape.size(); ++i) {
        EXPECT_EQ(int, chunk_shape[i].get<int>(), expected_chunk_shape[i]);
    }
}

void
verify_chunk_key_encoding(const nlohmann::json& metadata)
{
    EXPECT(metadata.contains("chunk_key_encoding"),
           "Expected key 'chunk_key_encoding' in metadata");
    const auto& chunk_key_encoding = metadata["chunk_key_encoding"];
    EXPECT(chunk_key_encoding.is_object(),
           "Expected chunk_key_encoding to be an object");

    EXPECT(chunk_key_encoding.contains("name"),
           "Expected key 'name' in chunk_key_encoding");
    auto chunk_key_encoding_name =
      chunk_key_encoding["name"].get<std::string>();
    EXPECT(chunk_key_encoding_name == "default",
           "Expected chunk_key_encoding name to be 'default', got '",
           chunk_key_encoding_name,
           "'");

    EXPECT(chunk_key_encoding.contains("configuration"),
           "Expected key 'configuration' in chunk_key_encoding");
    const auto& chunk_key_encoding_config = chunk_key_encoding["configuration"];
    EXPECT(chunk_key_encoding_config.is_object(),
           "Expected chunk_key_encoding configuration to be an object");

    EXPECT(chunk_key_encoding_config.contains("separator"),
           "Expected key 'separator' in chunk_key_encoding configuration");
    auto separator = chunk_key_encoding_config["separator"].get<std::string>();
    EXPECT(
      separator == "/", "Expected separator to be '/', got '", separator, "'");
}

void
verify_codecs(const nlohmann::json& metadata,
              const std::vector<int>& expected_chunk_shape,
              bool has_blosc_codec = false,
              int clevel = 2,
              const std::string& cname = "lz4",
              const std::string& shuffle = "bitshuffle",
              int typesize = 4)
{
    EXPECT(metadata.contains("codecs"), "Expected key 'codecs' in metadata");
    const auto& codecs = metadata["codecs"];
    EXPECT(codecs.is_array(), "Expected codecs to be an array");
    EXPECT_EQ(size_t, codecs.size(), 1);

    const auto& codec = codecs[0];
    EXPECT(codec.is_object(), "Expected codec to be an object");
    EXPECT(codec.contains("name"), "Expected key 'name' in codec");
    auto codec_name = codec["name"].get<std::string>();
    EXPECT(codec_name == "sharding_indexed",
           "Expected codec name to be 'sharding_indexed', got '",
           codec_name,
           "'");

    EXPECT(codec.contains("configuration"),
           "Expected key 'configuration' in codec");
    const auto& codec_config = codec["configuration"];
    EXPECT(codec_config.is_object(),
           "Expected codec configuration to be an object");

    // Verify chunk_shape
    EXPECT(codec_config.contains("chunk_shape"),
           "Expected key 'chunk_shape' in codec configuration");
    const auto& chunk_shape = codec_config["chunk_shape"];
    EXPECT(chunk_shape.is_array(), "Expected chunk_shape to be an array");
    EXPECT_EQ(size_t, chunk_shape.size(), expected_chunk_shape.size());
    for (size_t i = 0; i < expected_chunk_shape.size(); ++i) {
        EXPECT_EQ(int, chunk_shape[i].get<int>(), expected_chunk_shape[i]);
    }

    // Verify index_location
    EXPECT(codec_config.contains("index_location"),
           "Expected key 'index_location' in codec configuration");
    auto index_location = codec_config["index_location"].get<std::string>();
    EXPECT(index_location == "end",
           "Expected index_location to be 'end', got '",
           index_location,
           "'");

    // Verify codecs array
    EXPECT(codec_config.contains("codecs"),
           "Expected key 'codecs' in codec configuration");
    const auto& inner_codecs = codec_config["codecs"];
    EXPECT(inner_codecs.is_array(), "Expected codecs to be an array");

    int expected_inner_codecs_size = has_blosc_codec ? 2 : 1;
    EXPECT_EQ(size_t, inner_codecs.size(), expected_inner_codecs_size);

    // First codec should always be bytes
    const auto& bytes_codec = inner_codecs[0];
    EXPECT(bytes_codec.contains("name"), "Expected key 'name' in bytes codec");
    auto bytes_codec_name = bytes_codec["name"].get<std::string>();
    EXPECT(bytes_codec_name == "bytes",
           "Expected bytes codec name to be 'bytes', got '",
           bytes_codec_name,
           "'");

    // Verify blosc codec if present
    if (has_blosc_codec) {
        const auto& blosc_codec = inner_codecs[1];
        EXPECT(blosc_codec.contains("name"),
               "Expected key 'name' in blosc codec");
        auto blosc_codec_name = blosc_codec["name"].get<std::string>();
        EXPECT(blosc_codec_name == "blosc",
               "Expected blosc codec name to be 'blosc', got '",
               blosc_codec_name,
               "'");

        EXPECT(blosc_codec.contains("configuration"),
               "Expected key 'configuration' in blosc codec");
        const auto& blosc_config = blosc_codec["configuration"];
        EXPECT(blosc_config.is_object(),
               "Expected blosc configuration to be an object");

        EXPECT_EQ(int, blosc_config["blocksize"].get<int>(), 0);
        EXPECT_EQ(int, blosc_config["clevel"].get<int>(), clevel);
        EXPECT(blosc_config["cname"].get<std::string>() == cname,
               "Expected cname to be '",
               cname,
               "', got '",
               blosc_config["cname"].get<std::string>(),
               "'");
        EXPECT(blosc_config["shuffle"].get<std::string>() == shuffle,
               "Expected shuffle to be '",
               shuffle,
               "', got '",
               blosc_config["shuffle"].get<std::string>(),
               "'");
        EXPECT_EQ(int, blosc_config["typesize"].get<int>(), typesize);
    }

    // Verify index_codecs
    EXPECT(codec_config.contains("index_codecs"),
           "Expected key 'index_codecs' in codec configuration");
    const auto& index_codecs = codec_config["index_codecs"];
    EXPECT(index_codecs.is_array(), "Expected index_codecs to be an array");
    EXPECT_EQ(size_t, index_codecs.size(), 2);

    // First index codec should be bytes
    const auto& index_bytes_codec = index_codecs[0];
    EXPECT(index_bytes_codec.contains("name"),
           "Expected key 'name' in index bytes codec");
    auto index_bytes_codec_name = index_bytes_codec["name"].get<std::string>();
    EXPECT(index_bytes_codec_name == "bytes",
           "Expected index bytes codec name to be 'bytes', got '",
           index_bytes_codec_name,
           "'");

    EXPECT(index_bytes_codec.contains("configuration"),
           "Expected key 'configuration' in index bytes codec");
    const auto& index_bytes_config = index_bytes_codec["configuration"];
    EXPECT(index_bytes_config.contains("endian"),
           "Expected key 'endian' in index bytes codec configuration");
    auto index_bytes_endian = index_bytes_config["endian"].get<std::string>();
    EXPECT(index_bytes_endian == "little",
           "Expected index bytes endian to be 'little', got '",
           index_bytes_endian,
           "'");

    // Second index codec should be crc32c
    const auto& index_crc32c_codec = index_codecs[1];
    EXPECT(index_crc32c_codec.contains("name"),
           "Expected key 'name' in index crc32c codec");
    auto index_crc32c_codec_name =
      index_crc32c_codec["name"].get<std::string>();
    EXPECT(index_crc32c_codec_name == "crc32c",
           "Expected index crc32c codec name to be 'crc32c', got '",
           index_crc32c_codec_name,
           "'");
}

void
verify_labels_array_metadata(const nlohmann::json& metadata)
{
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = metadata["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(metadata.contains("node_type"),
           "Expected key 'node_type' in metadata");
    auto node_type = metadata["node_type"].get<std::string>();
    EXPECT(node_type == "array",
           "Expected node_type to be 'array', got '",
           node_type,
           "'");

    EXPECT(metadata.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(metadata["attributes"].is_object(),
           "Expected attributes to be an object");
    EXPECT(metadata["attributes"].empty(), "Expected attributes to be empty");

    EXPECT(metadata.contains("data_type"),
           "Expected key 'data_type' in metadata");
    auto data_type = metadata["data_type"].get<std::string>();
    EXPECT(data_type == "uint16",
           "Expected data_type to be 'uint16', got '",
           data_type,
           "'");

    EXPECT(metadata.contains("storage_transformers"),
           "Expected key 'storage_transformers' in metadata");
    EXPECT(metadata["storage_transformers"].is_array(),
           "Expected storage_transformers to be an array");
    EXPECT(metadata["storage_transformers"].empty(),
           "Expected storage_transformers to be empty");

    EXPECT(metadata.contains("fill_value"),
           "Expected key 'fill_value' in metadata");
    auto fill_value = metadata["fill_value"].get<int>();
    EXPECT(fill_value == 0, "Expected fill_value to be 0, got ", fill_value);

    verify_shape(metadata, { 6, 3, 4, 48, 64 });
    verify_dimension_names(metadata, { "t", "c", "z", "y", "x" });
    verify_chunk_grid(metadata, { 3, 3, 4, 48, 32 });
    verify_chunk_key_encoding(metadata);
    verify_codecs(metadata, { 3, 1, 2, 16, 16 }, false);
}

void
verify_multiscale_axes(const nlohmann::json& multiscale)
{
    EXPECT(multiscale.contains("axes"), "Expected key 'axes' in multiscale");
    const auto& axes = multiscale["axes"];
    EXPECT(axes.is_array(), "Expected axes to be an array");
    EXPECT_EQ(size_t, axes.size(), 4);

    // Time axis
    const auto& t_axis = axes[0];
    EXPECT(t_axis["name"].get<std::string>() == "t",
           "Expected first axis name to be 't'");
    EXPECT(t_axis["type"].get<std::string>() == "time",
           "Expected first axis type to be 'time'");

    // Z axis
    const auto& z_axis = axes[1];
    EXPECT(z_axis["name"].get<std::string>() == "z",
           "Expected second axis name to be 'z'");
    EXPECT(z_axis["type"].get<std::string>() == "space",
           "Expected second axis type to be 'space'");
    EXPECT(z_axis["unit"].get<std::string>() == "millimeter",
           "Expected second axis unit to be 'millimeter'");

    // Y axis
    const auto& y_axis = axes[2];
    EXPECT(y_axis["name"].get<std::string>() == "y",
           "Expected third axis name to be 'y'");
    EXPECT(y_axis["type"].get<std::string>() == "space",
           "Expected third axis type to be 'space'");
    EXPECT(y_axis["unit"].get<std::string>() == "micrometer",
           "Expected third axis unit to be 'micrometer'");

    // X axis
    const auto& x_axis = axes[3];
    EXPECT(x_axis["name"].get<std::string>() == "x",
           "Expected fourth axis name to be 'x'");
    EXPECT(x_axis["type"].get<std::string>() == "space",
           "Expected fourth axis type to be 'space'");
    EXPECT(x_axis["unit"].get<std::string>() == "micrometer",
           "Expected fourth axis unit to be 'micrometer'");
}

void
verify_coordinate_transformations(const nlohmann::json& dataset,
                                  const std::vector<double>& expected_scale)
{
    EXPECT(dataset.contains("coordinateTransformations"),
           "Expected key 'coordinateTransformations' in dataset");
    const auto& coord_transforms = dataset["coordinateTransformations"];
    EXPECT(coord_transforms.is_array(),
           "Expected coordinateTransformations to be an array");
    EXPECT_EQ(size_t, coord_transforms.size(), 1);

    const auto& transform = coord_transforms[0];
    EXPECT(transform.contains("type"),
           "Expected key 'type' in coordinate transformation");
    auto type = transform["type"].get<std::string>();
    EXPECT(type == "scale",
           "Expected coordinate transformation type to be 'scale', got '",
           type,
           "'");

    EXPECT(transform.contains("scale"),
           "Expected key 'scale' in coordinate transformation");
    const auto& scale = transform["scale"];
    EXPECT(scale.is_array(), "Expected scale to be an array");
    EXPECT_EQ(size_t, scale.size(), expected_scale.size());

    for (size_t i = 0; i < expected_scale.size(); ++i) {
        EXPECT_EQ(double, scale[i].get<double>(), expected_scale[i]);
    }
}

void
verify_multiscale_datasets(const nlohmann::json& multiscale)
{
    EXPECT(multiscale.contains("datasets"),
           "Expected key 'datasets' in multiscale");
    const auto& datasets = multiscale["datasets"];
    EXPECT(datasets.is_array(), "Expected datasets to be an array");
    EXPECT_EQ(size_t, datasets.size(), 3);

    // Dataset 0 (LOD0)
    const auto& dataset0 = datasets[0];
    EXPECT(dataset0.contains("path"), "Expected key 'path' in dataset 0");
    EXPECT(dataset0["path"].get<std::string>() == "0",
           "Expected dataset 0 path to be '0'");
    verify_coordinate_transformations(dataset0, { 1.0, 1.0, 1.0, 1.0 });

    // Dataset 1 (LOD1)
    const auto& dataset1 = datasets[1];
    EXPECT(dataset1.contains("path"), "Expected key 'path' in dataset 1");
    EXPECT(dataset1["path"].get<std::string>() == "1",
           "Expected dataset 1 path to be '1'");
    verify_coordinate_transformations(dataset1, { 1.0, 2.0, 2.0, 2.0 });

    // Dataset 2 (LOD2)
    const auto& dataset2 = datasets[2];
    EXPECT(dataset2.contains("path"), "Expected key 'path' in dataset 2");
    EXPECT(dataset2["path"].get<std::string>() == "2",
           "Expected dataset 2 path to be '2'");
    verify_coordinate_transformations(dataset2, { 1.0, 2.0, 4.0, 4.0 });
}

void
verify_multiscale_metadata(const nlohmann::json& multiscale)
{
    EXPECT(multiscale.contains("metadata"),
           "Expected key 'metadata' in multiscale");
    const auto& metadata = multiscale["metadata"];
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("method"), "Expected key 'method' in metadata");
    auto method = metadata["method"].get<std::string>();
    EXPECT(method == "np.ndarray.__getitem__",
           "Expected method to be 'np.ndarray.__getitem__', got '",
           method,
           "'");

    EXPECT(metadata.contains("version"), "Expected key 'version' in metadata");
    auto version = metadata["version"].get<std::string>();
    EXPECT(version == "2.2.6",
           "Expected version to be '2.2.6', got '",
           version,
           "'");

    EXPECT(metadata.contains("description"),
           "Expected key 'description' in metadata");
    auto description = metadata["description"].get<std::string>();
    EXPECT(
      description ==
        "Subsampling by taking every 2nd pixel/voxel (top-left corner of each "
        "2x2 block). Equivalent to numpy array slicing with stride 2.",
      "Expected specific description text");

    EXPECT(metadata.contains("args"), "Expected key 'args' in metadata");
    const auto& args = metadata["args"];
    EXPECT(args.is_array(), "Expected args to be an array");
    EXPECT_EQ(size_t, args.size(), 1);
    auto arg = args[0].get<std::string>();
    EXPECT(arg == "(slice(0, None, 2), slice(0, None, 2))",
           "Expected specific args text, got '",
           arg,
           "'");
}

void
verify_multiscale(const nlohmann::json& multiscale)
{
    EXPECT(multiscale.is_object(), "Expected multiscale to be an object");

    EXPECT(multiscale.contains("type"), "Expected key 'type' in multiscale");
    auto type = multiscale["type"].get<std::string>();
    EXPECT(type == "decimate",
           "Expected multiscale type to be 'decimate', got '",
           type,
           "'");

    verify_multiscale_axes(multiscale);
    verify_multiscale_datasets(multiscale);
    verify_multiscale_metadata(multiscale);
}

void
verify_ome_attributes(const nlohmann::json& attributes)
{
    EXPECT(attributes.contains("ome"), "Expected key 'ome' in attributes");
    const auto& ome = attributes["ome"];
    EXPECT(ome.is_object(), "Expected ome to be an object");

    EXPECT(ome.contains("name"), "Expected key 'name' in ome");
    auto name = ome["name"].get<std::string>();
    EXPECT(name == "/", "Expected ome name to be '/', got '", name, "'");

    EXPECT(ome.contains("version"), "Expected key 'version' in ome");
    auto version = ome["version"].get<std::string>();
    EXPECT(version == "0.5",
           "Expected ome version to be '0.5', got '",
           version,
           "'");

    EXPECT(ome.contains("multiscales"), "Expected key 'multiscales' in ome");
    const auto& multiscales = ome["multiscales"];
    EXPECT(multiscales.is_array(), "Expected multiscales to be an array");
    EXPECT_EQ(size_t, multiscales.size(), 1);

    verify_multiscale(multiscales[0]);
}

void
verify_multiscale_array_metadata(const nlohmann::json& metadata)
{
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = metadata["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(metadata.contains("node_type"),
           "Expected key 'node_type' in metadata");
    auto node_type = metadata["node_type"].get<std::string>();
    EXPECT(node_type == "group",
           "Expected node_type to be 'group', got '",
           node_type,
           "'");

    EXPECT(metadata.contains("consolidated_metadata"),
           "Expected key 'consolidated_metadata' in metadata");
    EXPECT(metadata["consolidated_metadata"].is_null(),
           "Expected consolidated_metadata to be null");

    EXPECT(metadata.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(metadata["attributes"].is_object(),
           "Expected attributes to be an object");

    verify_ome_attributes(metadata["attributes"]);
}

void
verify_array1_lod0_metadata(const nlohmann::json& metadata)
{
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = metadata["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(metadata.contains("node_type"),
           "Expected key 'node_type' in metadata");
    auto node_type = metadata["node_type"].get<std::string>();
    EXPECT(node_type == "array",
           "Expected node_type to be 'array', got '",
           node_type,
           "'");

    EXPECT(metadata.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(metadata["attributes"].is_object(),
           "Expected attributes to be an object");
    EXPECT(metadata["attributes"].empty(), "Expected attributes to be empty");

    EXPECT(metadata.contains("data_type"),
           "Expected key 'data_type' in metadata");
    auto data_type = metadata["data_type"].get<std::string>();
    EXPECT(data_type == "uint8",
           "Expected data_type to be 'uint8', got '",
           data_type,
           "'");

    EXPECT(metadata.contains("storage_transformers"),
           "Expected key 'storage_transformers' in metadata");
    EXPECT(metadata["storage_transformers"].is_array(),
           "Expected storage_transformers to be an array");
    EXPECT(metadata["storage_transformers"].empty(),
           "Expected storage_transformers to be empty");

    EXPECT(metadata.contains("fill_value"),
           "Expected key 'fill_value' in metadata");
    auto fill_value = metadata["fill_value"].get<int>();
    EXPECT(fill_value == 0, "Expected fill_value to be 0, got ", fill_value);

    verify_shape(metadata, { 10, 6, 48, 64 });
    verify_dimension_names(metadata, { "t", "z", "y", "x" });
    verify_chunk_grid(metadata, { 5, 6, 16, 16 });
    verify_chunk_key_encoding(metadata);
    verify_codecs(metadata, { 5, 3, 16, 16 }, false);
}

void
verify_array1_lod1_metadata(const nlohmann::json& metadata)
{
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = metadata["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(metadata.contains("node_type"),
           "Expected key 'node_type' in metadata");
    auto node_type = metadata["node_type"].get<std::string>();
    EXPECT(node_type == "array",
           "Expected node_type to be 'array', got '",
           node_type,
           "'");

    EXPECT(metadata.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(metadata["attributes"].is_object(),
           "Expected attributes to be an object");
    EXPECT(metadata["attributes"].empty(), "Expected attributes to be empty");

    EXPECT(metadata.contains("data_type"),
           "Expected key 'data_type' in metadata");
    auto data_type = metadata["data_type"].get<std::string>();
    EXPECT(data_type == "uint8",
           "Expected data_type to be 'uint8', got '",
           data_type,
           "'");

    EXPECT(metadata.contains("storage_transformers"),
           "Expected key 'storage_transformers' in metadata");
    EXPECT(metadata["storage_transformers"].is_array(),
           "Expected storage_transformers to be an array");
    EXPECT(metadata["storage_transformers"].empty(),
           "Expected storage_transformers to be empty");

    EXPECT(metadata.contains("fill_value"),
           "Expected key 'fill_value' in metadata");
    auto fill_value = metadata["fill_value"].get<int>();
    EXPECT(fill_value == 0, "Expected fill_value to be 0, got ", fill_value);

    verify_shape(metadata, { 10, 3, 24, 32 });
    verify_dimension_names(metadata, { "t", "z", "y", "x" });
    verify_chunk_grid(metadata, { 5, 3, 16, 16 });
    verify_chunk_key_encoding(metadata);
    verify_codecs(metadata, { 5, 3, 16, 16 }, false);
}

void
verify_array1_lod2_metadata(const nlohmann::json& metadata)
{
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = metadata["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(metadata.contains("node_type"),
           "Expected key 'node_type' in metadata");
    auto node_type = metadata["node_type"].get<std::string>();
    EXPECT(node_type == "array",
           "Expected node_type to be 'array', got '",
           node_type,
           "'");

    EXPECT(metadata.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(metadata["attributes"].is_object(),
           "Expected attributes to be an object");
    EXPECT(metadata["attributes"].empty(), "Expected attributes to be empty");

    EXPECT(metadata.contains("data_type"),
           "Expected key 'data_type' in metadata");
    auto data_type = metadata["data_type"].get<std::string>();
    EXPECT(data_type == "uint8",
           "Expected data_type to be 'uint8', got '",
           data_type,
           "'");

    EXPECT(metadata.contains("storage_transformers"),
           "Expected key 'storage_transformers' in metadata");
    EXPECT(metadata["storage_transformers"].is_array(),
           "Expected storage_transformers to be an array");
    EXPECT(metadata["storage_transformers"].empty(),
           "Expected storage_transformers to be empty");

    EXPECT(metadata.contains("fill_value"),
           "Expected key 'fill_value' in metadata");
    auto fill_value = metadata["fill_value"].get<int>();
    EXPECT(fill_value == 0, "Expected fill_value to be 0, got ", fill_value);

    verify_shape(metadata, { 10, 3, 12, 16 });
    verify_dimension_names(metadata, { "t", "z", "y", "x" });
    verify_chunk_grid(metadata, { 5, 3, 12, 16 });
    verify_chunk_key_encoding(metadata);
    verify_codecs(metadata, { 5, 3, 12, 16 }, false);
}

void
verify_array2_metadata(const nlohmann::json& metadata)
{
    EXPECT(metadata.is_object(), "Expected metadata to be an object");

    EXPECT(metadata.contains("zarr_format"),
           "Expected key 'zarr_format' in metadata");
    auto zarr_format = metadata["zarr_format"].get<int>();
    EXPECT(zarr_format == 3, "Expected zarr_format to be 3, got ", zarr_format);

    EXPECT(metadata.contains("node_type"),
           "Expected key 'node_type' in metadata");
    auto node_type = metadata["node_type"].get<std::string>();
    EXPECT(node_type == "array",
           "Expected node_type to be 'array', got '",
           node_type,
           "'");

    EXPECT(metadata.contains("attributes"),
           "Expected key 'attributes' in metadata");
    EXPECT(metadata["attributes"].is_object(),
           "Expected attributes to be an object");
    EXPECT(metadata["attributes"].empty(), "Expected attributes to be empty");

    EXPECT(metadata.contains("data_type"),
           "Expected key 'data_type' in metadata");
    auto data_type = metadata["data_type"].get<std::string>();
    EXPECT(data_type == "uint32",
           "Expected data_type to be 'uint32', got '",
           data_type,
           "'");

    EXPECT(metadata.contains("storage_transformers"),
           "Expected key 'storage_transformers' in metadata");
    EXPECT(metadata["storage_transformers"].is_array(),
           "Expected storage_transformers to be an array");
    EXPECT(metadata["storage_transformers"].empty(),
           "Expected storage_transformers to be empty");

    EXPECT(metadata.contains("fill_value"),
           "Expected key 'fill_value' in metadata");
    auto fill_value = metadata["fill_value"].get<int>();
    EXPECT(fill_value == 0, "Expected fill_value to be 0, got ", fill_value);

    verify_shape(metadata, { 9, 48, 64 });
    verify_dimension_names(metadata, { "z", "y", "x" });
    verify_chunk_grid(metadata, { 3, 16, 16 });
    verify_chunk_key_encoding(metadata);
    verify_codecs(metadata, { 3, 16, 16 }, true);
}

void
verify()
{
    // verify the intermediate group metadata at "", "path", and "path/to"
    {
        fs::path metadata_path = fs::path(test_path) / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json group_metadata = nlohmann::json::parse(f);
        verify_intermediate_group_metadata(group_metadata);
    }

    {
        fs::path metadata_path = fs::path(test_path) / "path" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json group_metadata = nlohmann::json::parse(f);
        verify_intermediate_group_metadata(group_metadata);
    }

    {
        fs::path metadata_path =
          fs::path(test_path) / "path" / "to" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json group_metadata = nlohmann::json::parse(f);
        verify_intermediate_group_metadata(group_metadata);
    }

    // verify the labels array metadata
    {
        fs::path metadata_path = fs::path(test_path) / "labels" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json labels_metadata = nlohmann::json::parse(f);
        verify_labels_array_metadata(labels_metadata);
    }

    // verify the group metadata for the first array
    {
        fs::path metadata_path =
          fs::path(test_path) / "path" / "to" / "array1" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json array1_metadata = nlohmann::json::parse(f);
        verify_multiscale_array_metadata(array1_metadata);
    }

    // verify the LODs for the first array
    {
        fs::path metadata_path =
          fs::path(test_path) / "path" / "to" / "array1" / "0" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(f);
        verify_array1_lod0_metadata(array_metadata);
    }

    {
        fs::path metadata_path =
          fs::path(test_path) / "path" / "to" / "array1" / "1" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(f);
        verify_array1_lod1_metadata(array_metadata);
    }

    {
        fs::path metadata_path =
          fs::path(test_path) / "path" / "to" / "array1" / "2" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(f);
        verify_array1_lod2_metadata(array_metadata);
    }

    // verify the second array metadata
    {
        fs::path metadata_path =
          fs::path(test_path) / "path" / "to" / "array2" / "zarr.json";
        EXPECT(fs::is_regular_file(metadata_path),
               "Expected file '",
               metadata_path,
               "' to exist");
        std::ifstream f(metadata_path);
        nlohmann::json array2_metadata = nlohmann::json::parse(f);
        verify_array2_metadata(array2_metadata);
    }
}

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    auto* stream = setup();

    std::vector<uint16_t> labels_frame(64 * 48, 0);
    const size_t bytes_of_frame_labels = labels_frame.size() * sizeof(uint16_t);
    // 2 chunks of 3 timepoints, 3 channels, 4 planes
    const size_t frames_to_acquire_labels = 72;

    std::vector<uint8_t> array1_frame(64 * 48, 1);
    const size_t bytes_of_frame_array1 = array1_frame.size() * sizeof(uint8_t);
    // 2 chunks of 5 timepoints, 6 planes
    const size_t frames_to_acquire_array1 = 60;

    std::vector<uint32_t> array2_frame(64 * 48, 2);
    const size_t bytes_of_frame_array2 = array2_frame.size() * sizeof(uint32_t);
    // 3 chunks of 3 planes
    const size_t frames_to_acquire_array2 = 9;

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire_labels; ++i) {
            ZarrStatusCode status = ZarrStream_append(stream,
                                                      labels_frame.data(),
                                                      bytes_of_frame_labels,
                                                      &bytes_out,
                                                      "labels");
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame_labels);
        }

        for (auto i = 0; i < frames_to_acquire_array1; ++i) {
            ZarrStatusCode status = ZarrStream_append(stream,
                                                      array1_frame.data(),
                                                      bytes_of_frame_array1,
                                                      &bytes_out,
                                                      "path/to/array1");
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame_array1);
        }

        for (auto i = 0; i < frames_to_acquire_array2; ++i) {
            ZarrStatusCode status = ZarrStream_append(stream,
                                                      array2_frame.data(),
                                                      bytes_of_frame_array2,
                                                      &bytes_out,
                                                      "path/to/array2");
            EXPECT(status == ZarrStatusCode_Success,
                   "Failed to append frame ",
                   i,
                   ": ",
                   Zarr_get_status_message(status));
            EXPECT_EQ(size_t, bytes_out, bytes_of_frame_array2);
        }

        ZarrStream_destroy(stream);

        verify();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    // cleanup
    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    return retval;
}
