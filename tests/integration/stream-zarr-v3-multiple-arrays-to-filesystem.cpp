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
        .version = ZarrVersion_3,
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
