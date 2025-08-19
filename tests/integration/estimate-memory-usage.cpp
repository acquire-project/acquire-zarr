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

void
test_current_memory_usage()
{
    ZarrStream* stream = nullptr;

    ZarrStreamSettings settings{
        .store_path = "test_store",
        .version = ZarrVersion_3,
        .overwrite = true,
    };

    const std::string output_key1 = "array1";
    const std::string output_key2 = "array2";
    const std::string output_key3 = "array3";

    const size_t frame_buffer_size =
      2 * array_height * array_width; // 2 bytes per pixel

    // Calculate exact array usage
    const size_t array1_usage =
      array_width * array_height * 3 * 32 * 2; // 64 * 48 * 3 * 32 * 2 bytes
    const size_t array2_usage =
      array_width * array_height * 3 * 32 * 2; // same as array1

    // For array3 with downsampling (LOD 0, 1, 2)
    const size_t lod0_usage = array_width * array_height * 3 * 32 * 2;
    const size_t lod1_usage = (array_width / 2) *
                              padded_size(array_height / 2, chunk_height) * 3 *
                              32 * 2;
    const size_t lod2_usage =
      (array_width / 4) * (array_height / 4) * 3 * 32 * 2;
    const size_t array3_usage = lod0_usage + lod1_usage + lod2_usage;

    size_t usage = 0;
    // a single timepoint, i.e., 3 channels
    std::vector<uint8_t> data(3 * frame_buffer_size, 0);

    // Test 1: Single uncompressed array
    {
        EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
                 ZarrStatusCode_Success,
               "Failed to create array settings");

        initialize_array(settings.arrays[0], output_key1, false, false);

        stream = ZarrStream_create(&settings);
        EXPECT(stream != nullptr, "Failed to create Zarr stream");

        ZarrStreamSettings_destroy_arrays(&settings);

        // Initial usage should be just frame buffer
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == frame_buffer_size,
               "Expected current memory usage ",
               frame_buffer_size,
               ", got ",
               usage);

        // Append 31 timepoints (don't flush yet) - each append is one timepoint
        // with all channels
        for (auto i = 0; i < 31; ++i) {
            size_t bytes_written;
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &bytes_written,
                                     output_key1.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Should now have frame buffer + array usage
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");
        EXPECT(usage == frame_buffer_size + array1_usage,
               "Expected current memory usage ",
               frame_buffer_size + array1_usage,
               ", got ",
               usage);

        // Add final timepoint to flush
        size_t bytes_written;
        EXPECT(ZarrStream_append(stream,
                                 data.data(),
                                 data.size(),
                                 &bytes_written,
                                 output_key1.c_str()) == ZarrStatusCode_Success,
               "Failed to append final frame");

        std::this_thread::sleep_for(std::chrono::milliseconds(750));

        // After flush, should be back to just frame buffer
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after flush");
        EXPECT(usage == frame_buffer_size,
               "Expected current memory usage after flush ",
               frame_buffer_size,
               ", got ",
               usage);

        ZarrStream_destroy(stream);

        if (fs::exists("test_store")) {
            fs::remove_all("test_store");
        }
    }

    // Test 2: Two arrays (uncompressed + compressed)
    {
        EXPECT(ZarrStreamSettings_create_arrays(&settings, 2) ==
                 ZarrStatusCode_Success,
               "Failed to create array settings");

        initialize_array(settings.arrays[0], output_key1, false, false);
        initialize_array(settings.arrays[1], output_key2, true, false);

        stream = ZarrStream_create(&settings);
        EXPECT(stream != nullptr, "Failed to create Zarr stream");

        delete settings.arrays[1].compression_settings;
        settings.arrays[1].compression_settings = nullptr;
        ZarrStreamSettings_destroy_arrays(&settings);

        // Initial usage: 2 frame buffers
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 2 * frame_buffer_size,
               "Expected current memory usage ",
               2 * frame_buffer_size,
               ", got ",
               usage);

        // Fill both arrays (31 timepoints each, don't flush)
        for (auto i = 0; i < 31; ++i) {
            size_t bytes_written;
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &bytes_written,
                                     output_key1.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append to array1");
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &bytes_written,
                                     output_key2.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append to array2");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Should have 2 frame buffers + both array usages
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 2 * frame_buffer_size + array1_usage + array2_usage,
               "Expected current memory usage ",
               2 * frame_buffer_size + array1_usage + array2_usage,
               ", got ",
               usage);

        // Flush array1
        size_t bytes_written;
        EXPECT(ZarrStream_append(stream,
                                 data.data(),
                                 data.size(),
                                 &bytes_written,
                                 output_key1.c_str()) == ZarrStatusCode_Success,
               "Failed to flush array1");

        std::this_thread::sleep_for(std::chrono::milliseconds(750));

        // Should have 2 frame buffers + array2 usage (array1 cleared)
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 2 * frame_buffer_size + array2_usage,
               "Expected current memory usage ",
               2 * frame_buffer_size + array2_usage,
               ", got ",
               usage);

        // Flush array2
        EXPECT(ZarrStream_append(stream,
                                 data.data(),
                                 data.size(),
                                 &bytes_written,
                                 output_key2.c_str()) == ZarrStatusCode_Success,
               "Failed to flush array2");

        std::this_thread::sleep_for(std::chrono::milliseconds(750));

        // Should be back to just 2 frame buffers
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 2 * frame_buffer_size,
               "Expected current memory usage ",
               2 * frame_buffer_size,
               ", got ",
               usage);

        ZarrStream_destroy(stream);

        if (fs::exists("test_store")) {
            fs::remove_all("test_store");
        }
    }

    // Test 3: Three arrays (uncompressed, compressed, compressed+multiscale)
    {
        EXPECT(ZarrStreamSettings_create_arrays(&settings, 3) ==
                 ZarrStatusCode_Success,
               "Failed to create array settings");

        initialize_array(settings.arrays[0], output_key1, false, false);
        initialize_array(settings.arrays[1], output_key2, true, false);
        initialize_array(settings.arrays[2], output_key3, true, true);

        stream = ZarrStream_create(&settings);
        EXPECT(stream != nullptr, "Failed to create Zarr stream");

        delete settings.arrays[1].compression_settings;
        delete settings.arrays[2].compression_settings;
        settings.arrays[1].compression_settings = nullptr;
        settings.arrays[2].compression_settings = nullptr;
        ZarrStreamSettings_destroy_arrays(&settings);

        // Initial usage: 3 frame buffers
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 3 * frame_buffer_size,
               "Expected current memory usage ",
               3 * frame_buffer_size,
               ", got ",
               usage);

        // Fill all arrays (31 timepoints each)
        for (auto i = 0; i < 31; ++i) {
            size_t bytes_written;
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &bytes_written,
                                     output_key1.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append to array1");
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &bytes_written,
                                     output_key2.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append to array2");
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &bytes_written,
                                     output_key3.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append to array3");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        // Should have 3 frame buffers + all array usages
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 3 * frame_buffer_size + array1_usage + array2_usage +
                          array3_usage,
               "Expected current memory usage ",
               3 * frame_buffer_size + array1_usage + array2_usage +
                 array3_usage,
               ", got ",
               usage);

        // Flush arrays one by one and test intermediate states
        size_t bytes_written;

        // Flush array1
        EXPECT(ZarrStream_append(stream,
                                 data.data(),
                                 data.size(),
                                 &bytes_written,
                                 output_key1.c_str()) == ZarrStatusCode_Success,
               "Failed to flush array1");
        std::this_thread::sleep_for(std::chrono::milliseconds(750));

        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 3 * frame_buffer_size + array2_usage + array3_usage,
               "Expected current memory usage ",
               3 * frame_buffer_size + array2_usage + array3_usage,
               ", got ",
               usage);

        // Flush array2
        EXPECT(ZarrStream_append(stream,
                                 data.data(),
                                 data.size(),
                                 &bytes_written,
                                 output_key2.c_str()) == ZarrStatusCode_Success,
               "Failed to flush array2");
        std::this_thread::sleep_for(std::chrono::milliseconds(750));

        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 3 * frame_buffer_size + array3_usage,
               "Expected current memory usage ",
               3 * frame_buffer_size + array3_usage,
               ", got ",
               usage);

        // Flush array3
        EXPECT(ZarrStream_append(stream,
                                 data.data(),
                                 data.size(),
                                 &bytes_written,
                                 output_key3.c_str()) == ZarrStatusCode_Success,
               "Failed to flush array3");
        std::this_thread::sleep_for(std::chrono::milliseconds(750));

        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");
        EXPECT(usage == 3 * frame_buffer_size,
               "Expected current memory usage ",
               3 * frame_buffer_size,
               ", got ",
               usage);

        ZarrStream_destroy(stream);

        if (fs::exists("test_store")) {
            fs::remove_all("test_store");
        }
    }
}

int
main()
{
    int retval = 1;

    try {
        test_max_memory_usage();
        test_current_memory_usage();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    if (fs::exists("test_store")) {
        fs::remove_all("test_store");
    }

    return retval;
}