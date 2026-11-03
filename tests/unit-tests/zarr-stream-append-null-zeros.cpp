#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <algorithm>
#include <filesystem>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

namespace fs = std::filesystem;

/**
 * ZarrStream_append with NULL data must write zeros, also after the stream has
 * written real frames. Frame-queue slots and the partial-frame staging buffer
 * are reused, so a NULL append that only resizes them would write whatever
 * frame they held last.
 */
namespace {
constexpr size_t array_width = 64, array_height = 48;
constexpr size_t frame_bytes = array_width * array_height;

// more than the largest frame queue (512 frames + 1 slot), so slots are reused
constexpr size_t n_real_frames = 600, n_null_frames = 600;

const std::string data_path =
  (fs::temp_directory_path() / TEST ".zarr").string();

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim++ = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 1,
        .shard_size_chunks = 1,
    };

    *dim++ = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_height,
        .chunk_size_px = array_height,
        .shard_size_chunks = 1,
    };

    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = array_width,
        .chunk_size_px = array_width,
        .shard_size_chunks = 1,
    };
}

void
append(ZarrStream* stream, const uint8_t* data, size_t n_bytes)
{
    size_t bytes_out = 0;
    EXPECT_EQ(int,
              ZarrStream_append(stream, data, n_bytes, &bytes_out, nullptr),
              ZarrStatusCode_Success);
    EXPECT_EQ(size_t, bytes_out, n_bytes);
}

/**
 * The frame for time point t. Each shard holds one uncompressed chunk, then an
 * index of (offset, nbytes) and a checksum. An all-zero chunk is not stored:
 * its index entry is empty and it reads as the fill value, 0.
 */
std::vector<uint8_t>
read_frame(size_t t)
{
    constexpr size_t index_bytes = 2 * sizeof(uint64_t) + sizeof(uint32_t);
    const fs::path path = fs::path(data_path) / "c" / std::to_string(t) / "0" /
                          "0";
    EXPECT(fs::is_regular_file(path), "missing shard ", path.string());

    std::ifstream file(path, std::ios::binary);
    std::vector<uint8_t> shard(fs::file_size(path));
    file.read(reinterpret_cast<char*>(shard.data()), shard.size());
    CHECK(file.good());
    CHECK(shard.size() >= index_bytes);

    uint64_t index[2];
    memcpy(index, shard.data() + shard.size() - index_bytes, sizeof(index));
    if (index[0] == std::numeric_limits<uint64_t>::max()) {
        EXPECT_EQ(size_t, shard.size(), index_bytes);
        return std::vector<uint8_t>(frame_bytes, 0);
    }

    EXPECT_EQ(size_t, index[0], 0);
    EXPECT_EQ(size_t, index[1], frame_bytes);
    return { shard.begin(), shard.begin() + frame_bytes };
}

/** Count the nonzero bytes of the frames in [begin, end). */
size_t
count_nonzero(size_t begin, size_t end, size_t& first_bad_frame)
{
    size_t n = 0;
    first_bad_frame = end;
    for (size_t t = begin; t < end; ++t) {
        for (const uint8_t b : read_frame(t)) {
            if (b != 0) {
                ++n;
                first_bad_frame = std::min(first_bad_frame, t);
            }
        }
    }
    return n;
}
} // namespace

int
main()
{
    int retval = 1;

    ZarrStream* stream = nullptr;
    ZarrStreamSettings settings = {};

    std::error_code ec;
    fs::remove_all(data_path, ec);

    settings.store_path = data_path.c_str();
    settings.max_threads = 0;

    ZarrStreamSettings_create_arrays(&settings, 1);
    settings.arrays->data_type = ZarrDataType_uint8;

    try {
        configure_stream_dimensions(settings.arrays);
        stream = ZarrStream_create(&settings);
        CHECK(nullptr != stream);

        // whole frames: real data, then NULL
        std::vector<uint8_t> frame(frame_bytes);
        for (size_t t = 0; t < n_real_frames; ++t) {
            std::fill(frame.begin(), frame.end(), uint8_t(1 + t % 250));
            append(stream, frame.data(), frame_bytes);
        }
        for (size_t t = 0; t < n_null_frames; ++t) {
            append(stream, nullptr, frame_bytes);
        }

        // partial frames: one real frame in two halves, then one NULL frame
        // in two halves, so the second goes through the same staging buffer
        std::fill(frame.begin(), frame.end(), uint8_t(0xEE));
        append(stream, frame.data(), frame_bytes / 2);
        append(stream, frame.data() + frame_bytes / 2, frame_bytes / 2);
        append(stream, nullptr, frame_bytes / 2);
        append(stream, nullptr, frame_bytes / 2);

        EXPECT_EQ(int, ZarrStream_close(stream), ZarrStatusCode_Success);
        stream = nullptr;

        for (size_t t = 0; t < n_real_frames; ++t) {
            for (const uint8_t b : read_frame(t)) {
                EXPECT_EQ(int, b, 1 + t % 250);
            }
        }

        size_t first_bad = 0;
        const size_t whole_nonzero =
          count_nonzero(n_real_frames, n_real_frames + n_null_frames, first_bad);
        if (whole_nonzero) {
            LOG_ERROR("NULL whole-frame appends: ",
                      whole_nonzero,
                      " nonzero bytes, first in frame ",
                      first_bad);
        }

        const size_t partial_t = n_real_frames + n_null_frames;
        for (const uint8_t b : read_frame(partial_t)) {
            EXPECT_EQ(int, b, 0xEE);
        }
        const size_t partial_nonzero =
          count_nonzero(partial_t + 1, partial_t + 2, first_bad);
        if (partial_nonzero) {
            LOG_ERROR("NULL partial-frame appends: ",
                      partial_nonzero,
                      " nonzero bytes");
        }

        CHECK(whole_nonzero == 0);
        CHECK(partial_nonzero == 0);
        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    if (stream) {
        ZarrStream_destroy(stream);
    }

    if (settings.arrays) {
        ZarrStreamSettings_destroy_arrays(&settings);
    }

    fs::remove_all(data_path, ec);
    return retval;
}
