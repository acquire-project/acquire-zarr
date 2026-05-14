#include "acquire.zarr.h"
#include "test.macros.hh"

#include <cstring>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const size_t array_width = 64, array_height = 48;
const size_t chunk_width = 16, chunk_height = 16;
} // namespace

void
initialize_array(ZarrArraySettings& settings,
                 const std::string& output_key,
                 bool compress,
                 bool multiscale)
{
    memset(&settings, 0, sizeof(settings));

    settings.output_key = output_key.c_str();
    settings.data_type = ZarrDataType_uint16;

    if (compress) {
        settings.compression_settings = new ZarrCompressionSettings;
        settings.compression_settings->compressor = ZarrCompressor_Blosc1;
        settings.compression_settings->codec = ZarrCompressionCodec_BloscLZ4;
        settings.compression_settings->level = 1;
        settings.compression_settings->shuffle = 1;
    }

    if (multiscale) {
        settings.multiscale = true;
        settings.downsampling_method = ZarrDownsamplingMethod_Decimate;
    } else {
        settings.multiscale = false;
    }

    EXPECT(ZarrArraySettings_create_dimension_array(&settings, 4) ==
             ZarrStatusCode_Success,
           "Failed to create dimension array");

    settings.dimensions[0] = { "time", ZarrDimensionType_Time, 0, 32, 1, "s",
                               1.0 };
    settings.dimensions[1] = {
        "channel", ZarrDimensionType_Channel, 3, 1, 1, "", 1.0
    };
    settings.dimensions[2] = {
        "height", ZarrDimensionType_Space, array_height, chunk_height, 1, "px",
        1.0
    };
    settings.dimensions[3] = {
        "width", ZarrDimensionType_Space, array_width, chunk_width, 1, "px", 1.0
    };
}

void
cleanup_compression(ZarrArraySettings& settings)
{
    delete settings.compression_settings;
    settings.compression_settings = nullptr;
}

void
test_one_uncompressed_array()
{
    ZarrStreamSettings settings{};
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "Failed to create array settings");

    initialize_array(settings.arrays[0], "arr", false, false);

    size_t usage = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(&settings, &usage) ==
             ZarrStatusCode_Success,
           "Estimate failed for one uncompressed array");
    EXPECT(usage > 0, "Expected nonzero memory estimate");

    LOG_INFO("one uncompressed array: ", usage, " bytes");

    ZarrStreamSettings_destroy_arrays(&settings);
}

void
test_compressed_more_than_uncompressed()
{
    ZarrStreamSettings settings{};

    // uncompressed
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "");
    initialize_array(settings.arrays[0], "arr", false, false);

    size_t usage_raw = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(
             &settings, &usage_raw) == ZarrStatusCode_Success,
           "");
    ZarrStreamSettings_destroy_arrays(&settings);

    // compressed
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "");
    initialize_array(settings.arrays[0], "arr", true, false);

    size_t usage_comp = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(
             &settings, &usage_comp) == ZarrStatusCode_Success,
           "");

    LOG_INFO("uncompressed: ", usage_raw, ", compressed: ", usage_comp);
    EXPECT(usage_comp > usage_raw,
           "Compressed array should require more memory than uncompressed");

    cleanup_compression(settings.arrays[0]);
    ZarrStreamSettings_destroy_arrays(&settings);
}

void
test_multiscale_more_than_single_scale()
{
    ZarrStreamSettings settings{};

    // single-scale compressed
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "");
    initialize_array(settings.arrays[0], "arr", true, false);

    size_t usage_single = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(
             &settings, &usage_single) == ZarrStatusCode_Success,
           "");
    cleanup_compression(settings.arrays[0]);
    ZarrStreamSettings_destroy_arrays(&settings);

    // multiscale compressed
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "");
    initialize_array(settings.arrays[0], "arr", true, true);

    size_t usage_multi = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(
             &settings, &usage_multi) == ZarrStatusCode_Success,
           "");

    LOG_INFO("single-scale: ", usage_single, ", multiscale: ", usage_multi);
    EXPECT(usage_multi > usage_single,
           "Multiscale should require more memory than single-scale");

    cleanup_compression(settings.arrays[0]);
    ZarrStreamSettings_destroy_arrays(&settings);
}

void
test_more_arrays_more_memory()
{
    ZarrStreamSettings settings{};

    // one array
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "");
    initialize_array(settings.arrays[0], "a", false, false);

    size_t usage_one = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(
             &settings, &usage_one) == ZarrStatusCode_Success,
           "");
    ZarrStreamSettings_destroy_arrays(&settings);

    // two arrays
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 2) ==
             ZarrStatusCode_Success,
           "");
    initialize_array(settings.arrays[0], "a", false, false);
    initialize_array(settings.arrays[1], "b", false, false);

    size_t usage_two = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(
             &settings, &usage_two) == ZarrStatusCode_Success,
           "");

    LOG_INFO("one array: ", usage_one, ", two arrays: ", usage_two);
    EXPECT(usage_two > usage_one,
           "Two arrays should require more memory than one");

    ZarrStreamSettings_destroy_arrays(&settings);
}

void
test_invalid_args()
{
    size_t usage = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(nullptr, &usage) !=
             ZarrStatusCode_Success,
           "Should fail with null settings");
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(nullptr, nullptr) !=
             ZarrStatusCode_Success,
           "Should fail with null settings and usage");

    ZarrStreamSettings settings{};
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(&settings, nullptr) !=
             ZarrStatusCode_Success,
           "Should fail with null usage");
}

int
main()
{
    int retval = 1;

    try {
        test_one_uncompressed_array();
        test_compressed_more_than_uncompressed();
        test_multiscale_more_than_single_scale();
        test_more_arrays_more_memory();
        test_invalid_args();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    return retval;
}
