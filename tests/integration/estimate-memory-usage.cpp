#include "acquire.zarr.h"
#include "test.macros.hh"

#include <cstring>
#include <filesystem>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {
const size_t array_width = 64, array_height = 48;
const size_t chunk_width = 16, chunk_height = 16;

size_t
padded_size(size_t size, size_t chunk_size)
{
    return chunk_size * ((size + chunk_size - 1) / chunk_size);
}
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
        settings.compression_settings->shuffle = 1; // enable shuffling
    }

    if (multiscale) {
        settings.multiscale = true;
        settings.downsampling_method = ZarrDownsamplingMethod_Decimate;
    } else {
        settings.multiscale = false;
    }

    // allocate 4 dimensions
    EXPECT(ZarrArraySettings_create_dimension_array(&settings, 4) ==
             ZarrStatusCode_Success,
           "Failed to create dimension array");
    EXPECT(settings.dimension_count == 4, "Dimension count mismatch");

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
test_max_memory_usage()
{
    ZarrStreamSettings settings{ 0 };

    // create settings for a Zarr stream with one array
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "Failed to create array settings");

    const std::string output_key1 = "test_array1";
    initialize_array(settings.arrays[0], output_key1, false, false);

    const size_t frame_queue_size = 1 << 30; // 1 GiB
    const size_t expected_frame_size = array_width * array_height * 2;

    const size_t padded_width = padded_size(array_width, chunk_width);
    const size_t padded_height = padded_size(array_height, chunk_height);
    const size_t padded_frame_size = 2 * padded_height * padded_width;
    const size_t expected_array_usage = padded_frame_size * // frame
                                        3 *                 // channels
                                        32;                 // time

    size_t usage = 0, expected_usage;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(&settings, &usage) ==
             ZarrStatusCode_Success,
           "Failed to estimate memory usage");

    //  for the array + each array's frame buffer
    expected_usage =
      frame_queue_size + expected_array_usage + expected_frame_size;
    EXPECT(usage == expected_usage,
           "Expected max memory usage ",
           expected_usage,
           ", got ",
           usage);

    ZarrStreamSettings_destroy_arrays(&settings);

    // create settings for a Zarr stream with two arrays, one compressed
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 2) ==
             ZarrStatusCode_Success,
           "Failed to create array settings");

    const std::string output_key2 = "test_array2";
    initialize_array(settings.arrays[0], output_key1, false, false);
    EXPECT(settings.arrays[0].dimension_count == 4, "Dimension count mismatch");

    initialize_array(settings.arrays[1], output_key2, true, false);
    EXPECT(settings.arrays[1].dimension_count == 4, "Dimension count mismatch");

    usage = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(&settings, &usage) ==
             ZarrStatusCode_Success,
           "Failed to estimate memory usage");

    // one uncompressed (1) and one compressed (2), plus each array's frame
    // buffer
    expected_usage =
      frame_queue_size + 3 * expected_array_usage + 2 * expected_frame_size;
    EXPECT(usage == expected_usage,
           "Expected max memory usage ",
           expected_usage,
           ", got ",
           usage);

    delete settings.arrays[1].compression_settings;
    settings.arrays[1].compression_settings = nullptr;

    ZarrStreamSettings_destroy_arrays(&settings);

    // create settings for a Zarr stream with three arrays, one compressed,
    // one compressed with downsampling, and one uncompressed
    EXPECT(ZarrStreamSettings_create_arrays(&settings, 3) ==
             ZarrStatusCode_Success,
           "Failed to create array settings");

    const std::string output_key3 = "test_array3";
    initialize_array(settings.arrays[0], output_key1, false, false);
    EXPECT(settings.arrays[0].dimension_count == 4, "Dimension count mismatch");

    initialize_array(settings.arrays[1], output_key2, true, false);
    EXPECT(settings.arrays[1].dimension_count == 4, "Dimension count mismatch");

    initialize_array(settings.arrays[2], output_key3, true, true);
    EXPECT(settings.arrays[2].dimension_count == 4, "Dimension count mismatch");

    usage = 0;
    EXPECT(ZarrStreamSettings_estimate_max_memory_usage(&settings, &usage) ==
             ZarrStatusCode_Success,
           "Failed to estimate memory usage");

    // one uncompressed (1), one compressed (2), one compressed with
    // downsampling (4), and 3 frame buffers
    expected_usage =
      frame_queue_size + 7 * expected_array_usage + 3 * expected_frame_size;
    EXPECT(usage == expected_usage,
           "Expected max memory usage ",
           expected_usage,
           ", got ",
           usage);

    delete settings.arrays[1].compression_settings;
    settings.arrays[1].compression_settings = nullptr;

    delete settings.arrays[2].compression_settings;
    settings.arrays[2].compression_settings = nullptr;

    ZarrStreamSettings_destroy_arrays(&settings);
}

int
main()
{
    int retval = 1;

    try {
        test_max_memory_usage();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    return retval;
}