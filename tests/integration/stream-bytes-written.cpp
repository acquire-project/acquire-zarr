#include "acquire.zarr.h"
#include "test.macros.hh"

#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace {
// 2 x 2 x 2 chunks of 2 x 16 x 16 uint16; 2 shards of 2 x 2 x 1 chunks
const unsigned int array_t = 4, array_y = 32, array_x = 32;
const unsigned int chunk_t = 2, chunk_y = 16, chunk_x = 16;
const unsigned int shard_t = 2, shard_y = 2, shard_x = 1;

ZarrStream*
make_stream(const std::string& store_path,
            ZarrCompressionSettings* compression,
            ZarrDownsamplingMethod downsampling)
{
    ZarrArraySettings array = {
        .compression_settings = compression,
        .data_type = ZarrDataType_uint16,
        .downsampling_method = downsampling,
    };
    ZarrStreamSettings settings = {
        .store_path = store_path.c_str(),
        .overwrite = true,
        .arrays = &array,
        .array_count = 1,
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(&array, 3));
    array.dimensions[0] =
      DIM("t", ZarrDimensionType_Time, array_t, chunk_t, shard_t, nullptr, 1.0);
    array.dimensions[1] = DIM(
      "y", ZarrDimensionType_Space, array_y, chunk_y, shard_y, nullptr, 1.0);
    array.dimensions[2] = DIM(
      "x", ZarrDimensionType_Space, array_x, chunk_x, shard_x, nullptr, 1.0);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(&array);
    return stream;
}

/**
 * @param zero_frames Frames 0 to zero_frames - 1 are all zero, so the chunks
 * that hold only those frames are not stored.
 */
uint64_t
append_frames_and_close(ZarrStream* stream, unsigned int zero_frames = 0)
{
    std::vector<uint16_t> frame(array_y * array_x);
    uint64_t previous = 0;
    for (unsigned int t = 0; t < array_t; ++t) {
        for (size_t j = 0; j < frame.size(); ++j) {
            frame[j] =
              t < zero_frames ? 0 : static_cast<uint16_t>(1 + t * 7 + (j % 97));
        }
        size_t bytes_out = 0;
        CHECK_OK(ZarrStream_append(stream,
                                   frame.data(),
                                   frame.size() * sizeof(uint16_t),
                                   &bytes_out,
                                   nullptr));

        uint64_t so_far = 0;
        CHECK_OK(ZarrStream_get_bytes_written(stream, &so_far));
        CHECK(so_far >= previous);
        previous = so_far;
    }

    uint64_t total = 0;
    CHECK_OK(ZarrStream_close_and_get_bytes_written(stream, &total));
    CHECK(total >= previous);
    return total;
}

void
test_raw_to_discard()
{
    auto* stream = make_stream(
      "discard://" TEST ".zarr", nullptr, ZarrDownsamplingMethod_None);
    CHECK(stream);

    const uint64_t chunk_bytes = chunk_t * chunk_y * chunk_x * sizeof(uint16_t);
    const uint64_t n_chunks =
      (array_t / chunk_t) * (array_y / chunk_y) * (array_x / chunk_x);
    const uint64_t chunks_per_shard = shard_t * shard_y * shard_x;
    const uint64_t n_shards = n_chunks / chunks_per_shard;
    // each index entry is an (offset, nbytes) pair of uint64, then a crc32c
    const uint64_t index_bytes = 2 * sizeof(uint64_t) * chunks_per_shard + 4;
    const uint64_t expected = n_chunks * chunk_bytes + n_shards * index_bytes;

    EXPECT_EQ(uint64_t, append_frames_and_close(stream), expected);

    // the first time chunk of every shard is all zero and is not stored
    stream = make_stream(
      "discard://" TEST ".zarr", nullptr, ZarrDownsamplingMethod_None);
    CHECK(stream);
    const uint64_t n_zero_chunks = n_chunks / (array_t / chunk_t);
    EXPECT_EQ(uint64_t,
              append_frames_and_close(stream, chunk_t),
              expected - n_zero_chunks * chunk_bytes);
}

void
test_compressed_multiscale_matches_disk(const fs::path& store)
{
    ZarrCompressionSettings compression = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 1,
        .shuffle = 1,
    };
    auto* stream =
      make_stream(store.string(), &compression, ZarrDownsamplingMethod_Mean);
    CHECK(stream);
    const uint64_t counted = append_frames_and_close(stream);

    stream = make_stream(
      "discard://" TEST ".zarr", &compression, ZarrDownsamplingMethod_Mean);
    CHECK(stream);
    EXPECT_EQ(uint64_t, append_frames_and_close(stream), counted);

    uint64_t on_disk = 0;
    for (const auto& entry : fs::recursive_directory_iterator(store)) {
        if (entry.is_regular_file() && entry.path().filename() != "zarr.json") {
            on_disk += entry.file_size();
        }
    }
    CHECK(on_disk > 0);
    EXPECT_EQ(uint64_t, counted, on_disk);
}
} // namespace

int
main()
{
    int retval = 1;
    const fs::path store = fs::temp_directory_path() / (TEST ".zarr");

    try {
        uint64_t bytes = 1;
        CHECK_OK(ZarrStream_close_and_get_bytes_written(nullptr, &bytes));
        EXPECT_EQ(uint64_t, bytes, 0);
        CHECK(ZarrStream_get_bytes_written(nullptr, &bytes) !=
              ZarrStatusCode_Success);

        test_raw_to_discard();
        test_compressed_multiscale_matches_disk(store);
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    fs::remove_all(store);
    return retval;
}
