#include "acquire.zarr.h"
#include "test.macros.hh"

#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
const unsigned int array_width = 64, array_height = 48, array_planes = 6,
                   array_timepoints = 10;
const unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2,
                   chunk_timepoints = 5;
const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                   shard_timepoints = 2;

ZarrStream*
make_stream(const char* store_path,
            ZarrS3Settings* s3_settings,
            bool overwrite = true)
{
    ZarrCompressionSettings compression = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscZstd,
        .level = 1,
        .shuffle = 1,
    };
    ZarrArraySettings array = {
        .compression_settings = &compression,
        .data_type = ZarrDataType_uint16,
        .downsampling_method = ZarrDownsamplingMethod_Mean,
    };
    ZarrStreamSettings settings = {
        .store_path = store_path,
        .s3_settings = s3_settings,
        .max_threads = 0,
        .overwrite = overwrite,
        .arrays = &array,
        .array_count = 1,
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(&array, 4));
    array.dimensions[0] = DIM("t",
                              ZarrDimensionType_Time,
                              array_timepoints,
                              chunk_timepoints,
                              shard_timepoints,
                              nullptr,
                              1.0);
    array.dimensions[1] = DIM("z",
                              ZarrDimensionType_Space,
                              array_planes,
                              chunk_planes,
                              shard_planes,
                              nullptr,
                              1.0);
    array.dimensions[2] = DIM("y",
                              ZarrDimensionType_Space,
                              array_height,
                              chunk_height,
                              shard_height,
                              nullptr,
                              1.0);
    array.dimensions[3] = DIM("x",
                              ZarrDimensionType_Space,
                              array_width,
                              chunk_width,
                              shard_width,
                              nullptr,
                              1.0);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(&array);
    return stream;
}
} // namespace

int
main()
{
    int retval = 1;
    const fs::path scratch = fs::temp_directory_path() / TEST;
    const fs::path original_cwd = fs::current_path();

    try {
        fs::remove_all(scratch);
        fs::create_directories(scratch);
        fs::current_path(scratch); // a stray relative write lands here

        ZarrStream* stream = make_stream("discard://" TEST ".zarr", nullptr);
        CHECK(stream);

        const size_t frame_px = array_width * array_height;
        std::vector<uint16_t> frame(frame_px);
        const size_t n_frames = array_planes * array_timepoints;
        for (size_t i = 0; i < n_frames; ++i) {
            for (size_t j = 0; j < frame_px; ++j) {
                frame[j] = static_cast<uint16_t>(i * 31 + j);
            }
            size_t bytes_out = 0;
            CHECK_OK(ZarrStream_append(stream,
                                       frame.data(),
                                       frame.size() * sizeof(uint16_t),
                                       &bytes_out,
                                       nullptr));
            EXPECT_EQ(size_t, bytes_out, frame.size() * sizeof(uint16_t));
        }

        CHECK_OK(ZarrStream_write_custom_metadata(
          stream, nullptr, "note", R"({"value":"discarded"})"));
        CHECK_OK(ZarrStream_close(stream));

        CHECK(fs::is_empty(scratch));

        // keep-existing path: probes for group metadata instead of replacing it
        // and the store path is trimmed before the scheme is tested
        stream = make_stream(" discard://" TEST ".zarr ", nullptr, false);
        CHECK(stream);
        CHECK_OK(ZarrStream_close(stream));
        CHECK(fs::is_empty(scratch));

        ZarrS3Settings s3 = {
            .endpoint = "http://localhost:9000",
            .bucket_name = "bucket",
        };
        EXPECT(make_stream("discard://" TEST ".zarr", &s3) == nullptr,
               "Expected a discard store with S3 settings to be rejected");

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    fs::current_path(original_cwd);
    fs::remove_all(scratch);
    return retval;
}
