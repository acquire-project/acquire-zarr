#include "acquire.zarr.h"
#include "test.macros.hh"

#include <cstring>
#include <filesystem>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {
const size_t array_width = 1080, array_height = 1920;
const size_t chunk_width = 128, chunk_height = 128;

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

    const std::string output_key1 = "test_array1";
    const std::string output_key2 = "test_array2";
    const std::string output_key3 = "test_array3";

    const size_t max_frame_queue_size = 1 << 30; // 1 GiB
    const size_t expected_frame_size = array_width * array_height * 2;

    const size_t padded_width = padded_size(array_width, chunk_width);
    const size_t padded_height = padded_size(array_height, chunk_height);
    const size_t padded_frame_size = 2 * padded_height * padded_width;
    const size_t expected_array_usage = padded_frame_size * // frame
                                        3 *                 // channels
                                        32;                 // time

    size_t usage = 0, max_expected_usage;
    std::vector<uint8_t> data(expected_frame_size, 0);

    // create a stream with a single uncompressed array
    {
        EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
                 ZarrStatusCode_Success,
               "Failed to create array settings");

        initialize_array(settings.arrays[0], output_key1, false, false);

        stream = ZarrStream_create(&settings);
        EXPECT(stream != nullptr, "Failed to create Zarr stream");

        ZarrStreamSettings_destroy_arrays(&settings);

        // check current memory usage
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");

        // we haven't appended any data yet, so usage should be just the frame
        // queue and the array-level frame buffer
        max_expected_usage = max_frame_queue_size + expected_frame_size;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        // append some data to the stream, but not enough to flush the chunks
        for (auto i = 0; i < 31; ++i) {
            std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &usage,
                                     output_key1.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
            EXPECT(usage == data.size(),
                   "Expected bytes written to match input size");
        }

        // give it a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // now check the usage again
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");

        // usage should now be the size of the frame queue plus the array's
        // frame buffer plus the size of all the chunks in memory
        max_expected_usage =
          max_frame_queue_size + expected_frame_size + expected_array_usage;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        ZarrStream_destroy(stream);
    }

    // create a stream with two arrays, one uncompressed and compressed
    {
        EXPECT(ZarrStreamSettings_create_arrays(&settings, 2) ==
                 ZarrStatusCode_Success,
               "Failed to create array settings");

        initialize_array(settings.arrays[0], output_key1, false, false);
        EXPECT(settings.arrays[0].dimension_count == 4,
               "Dimension count mismatch");

        initialize_array(settings.arrays[1], output_key2, true, false);
        EXPECT(settings.arrays[1].dimension_count == 4,
               "Dimension count mismatch");

        usage = 0;

        // create stream with these settings
        stream = ZarrStream_create(&settings);
        EXPECT(stream != nullptr, "Failed to create Zarr stream");

        delete settings.arrays[1].compression_settings;
        settings.arrays[1].compression_settings = nullptr;

        ZarrStreamSettings_destroy_arrays(&settings);

        // check current memory usage
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");

        // we haven't appended any data yet, so usage should be just two frame
        // queues and the array-level frame buffer
        max_expected_usage = max_frame_queue_size + 2 * expected_frame_size;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        // append some data to the first array
        for (auto i = 0; i < 31; ++i) { // not enough to flush
            std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &usage,
                                     output_key1.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
            EXPECT(usage == data.size(),
                   "Expected bytes written to match input size");
        }

        // give it a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // now check the usage again
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");

        // usage should now be the size of the frame queue plus each array's
        // frame buffer plus the size of all the chunks in memory for a single
        // array
        max_expected_usage =
          max_frame_queue_size + 2 * expected_frame_size + expected_array_usage;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        // append some data to the other array
        for (auto i = 0; i < 31; ++i) { // not enough to flush
            std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &usage,
                                     output_key2.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
            EXPECT(usage == data.size(),
                   "Expected bytes written to match input size");
        }

        // give it a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // now check the usage again
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");

        // usage should now be the size of the frame queue plus each array's
        // frame buffer plus the size of all the chunks in memory for both
        // arrays
        max_expected_usage = max_frame_queue_size + 2 * expected_frame_size +
                             2 * expected_array_usage;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        ZarrStream_destroy(stream);
    }

    // create a stream with three arrays, one compressed, one compressed with
    // downsampling, and one uncompressed
    {
        EXPECT(ZarrStreamSettings_create_arrays(&settings, 3) ==
                 ZarrStatusCode_Success,
               "Failed to create array settings");

        initialize_array(settings.arrays[0], output_key1, false, false);
        EXPECT(settings.arrays[0].dimension_count == 4,
               "Dimension count mismatch");

        initialize_array(settings.arrays[1], output_key2, true, false);
        EXPECT(settings.arrays[1].dimension_count == 4,
               "Dimension count mismatch");

        initialize_array(settings.arrays[2], output_key3, true, true);
        EXPECT(settings.arrays[2].dimension_count == 4,
               "Dimension count mismatch");

        usage = 0;

        // create stream with these settings
        stream = ZarrStream_create(&settings);
        EXPECT(stream != nullptr, "Failed to create Zarr stream");

        delete settings.arrays[1].compression_settings;
        settings.arrays[1].compression_settings = nullptr;

        delete settings.arrays[2].compression_settings;
        settings.arrays[2].compression_settings = nullptr;

        ZarrStreamSettings_destroy_arrays(&settings);

        // check current memory usage
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage");

        // we haven't appended any data yet, so usage should be just two frame
        // queues and the array-level frame buffer
        max_expected_usage = max_frame_queue_size + 2 * expected_frame_size;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        // append some data to the stream
        for (auto i = 0; i < 31; ++i) { // not enough to flush
            std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &usage,
                                     output_key1.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
            EXPECT(usage == data.size(),
                   "Expected bytes written to match input size");
        }

        // give it a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // now check the usage again
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");

        // usage should now be the size of the frame queue plus each array's
        // frame buffer plus the size of all the chunks in memory for the first
        // array
        max_expected_usage =
          max_frame_queue_size + 3 * expected_frame_size + expected_array_usage;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        // append some data to the second array
        for (auto i = 0; i < 31; ++i) { // not enough to flush
            std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &usage,
                                     output_key2.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
            EXPECT(usage == data.size(),
                   "Expected bytes written to match input size");
        }

        // give it a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // now check the usage again
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");

        // usage should now be the size of the frame queue plus each array's
        // frame buffer plus the size of all the chunks in memory for two
        // arrays
        max_expected_usage = max_frame_queue_size + 3 * expected_frame_size +
                             2 * expected_array_usage;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        // append some data to the third array
        for (auto i = 0; i < 31; ++i) { // not enough to flush
            std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
            EXPECT(ZarrStream_append(stream,
                                     data.data(),
                                     data.size(),
                                     &usage,
                                     output_key3.c_str()) ==
                     ZarrStatusCode_Success,
                   "Failed to append data to Zarr stream");
            EXPECT(usage == data.size(),
                   "Expected bytes written to match input size");
        }

        // give it a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // now check the usage again
        EXPECT(ZarrStream_get_current_memory_usage(stream, &usage) ==
                 ZarrStatusCode_Success,
               "Failed to get current memory usage after appending data");

        // usage should now be the size of the frame queue plus each array's
        // frame buffer plus the size of all the chunks in memory for two
        // arrays
        max_expected_usage = max_frame_queue_size + 3 * expected_frame_size +
                             4 * expected_array_usage;
        EXPECT(usage <= max_expected_usage,
               "Expected current memory usage ",
               max_expected_usage,
               ", got ",
               usage);

        ZarrStream_destroy(stream);
    }
}

void
check_one_thing()
{
    ZarrArraySettings array{
        .output_key = nullptr,
        .compression_settings = nullptr,
        .dimensions = new ZarrDimensionProperties[5],
        .dimension_count = 5,
        .data_type = ZarrDataType_uint16,
        .multiscale = false,
    };

    auto* dims = array.dimensions;
    dims[0] = { "time", ZarrDimensionType_Time, 0, 5, 1 };
    dims[1] = { "channel", ZarrDimensionType_Channel, 3, 1, 1 };
    dims[2] = { "planes", ZarrDimensionType_Space, 6, 2, 1 };
    dims[3] = { "height", ZarrDimensionType_Space, 48, 16, 1 };
    dims[4] = { "height", ZarrDimensionType_Space, 64, 16, 1 };

    ZarrStreamSettings settings{
        .store_path = "test_store",
        .version = ZarrVersion_3,
        .overwrite = true,
        .arrays = &array,
        .array_count = 1,
    };

    //    const size_t expected_array = 2 * padded_size(64, 16) *
    //                                  padded_size(48, 16) * padded_size(6, 3)
    //                                  * padded_size(3, 1) * 5;
    const size_t expected_array = 5 * 3 * 6 * 48 * 64 * 2;
    const size_t expected_frame = 64 * 48 * 2;
    const size_t frame_queue_size = 1 << 30; // 1 GiB

    size_t usage = 0;
    ZarrStreamSettings_estimate_max_memory_usage(&settings, &usage);

    EXPECT(usage == frame_queue_size + expected_array + expected_frame,
           "Expected max memory usage ",
           frame_queue_size + expected_array + expected_frame,
           ", got ",
           usage);

    delete[] array.dimensions;
}

int
main()
{
    int retval = 1;

    try {
        test_max_memory_usage();
        test_current_memory_usage();
        check_one_thing();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
    }

    if (fs::exists("test_store")) {
        fs::remove_all("test_store");
    }

    return retval;
}