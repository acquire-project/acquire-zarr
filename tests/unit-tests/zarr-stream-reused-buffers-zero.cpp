#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <vector>

namespace fs = std::filesystem;

/**
 * Chunk buffers are reused across layers, and a buffer can move between chunks
 * that leave different bytes unwritten: an interior chunk's buffer can next
 * hold an edge tile, the ragged last chunk of an internal dimension, or the
 * short final chunk of the append dimension. Every byte no frame writes must
 * still read as zero, as it did when each chunk had a fresh buffer.
 */
namespace {
// t is the append dimension, chunked by 2; z is ragged (3 planes, chunk 2);
// y and x leave edge tiles (30 x 50 px in 16 x 16 tiles)
constexpr size_t n_timepoints = 41; // odd, so the final t chunk is short
constexpr size_t n_planes = 3, height = 30, width = 50;
constexpr size_t chunk_t = 2, chunk_z = 2, chunk_y = 16, chunk_x = 16;

constexpr size_t chunks_t = (n_timepoints + chunk_t - 1) / chunk_t;
constexpr size_t chunks_z = (n_planes + chunk_z - 1) / chunk_z;
constexpr size_t chunks_y = (height + chunk_y - 1) / chunk_y;
constexpr size_t chunks_x = (width + chunk_x - 1) / chunk_x;

constexpr size_t frame_bytes = height * width;
constexpr size_t chunk_bytes = chunk_t * chunk_z * chunk_y * chunk_x;

const std::string data_path =
  (fs::temp_directory_path() / TEST ".zarr").string();

uint8_t
frame_value(size_t t, size_t z)
{
    return static_cast<uint8_t>(1 + (t * n_planes + z) % 250);
}

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 4));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim++ = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = chunk_t,
        .shard_size_chunks = 1,
    };

    *dim++ = ZarrDimensionProperties{
        .name = "z",
        .type = ZarrDimensionType_Space,
        .array_size_px = n_planes,
        .chunk_size_px = chunk_z,
        .shard_size_chunks = 1,
    };

    *dim++ = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = height,
        .chunk_size_px = chunk_y,
        .shard_size_chunks = 1,
    };

    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = width,
        .chunk_size_px = chunk_x,
        .shard_size_chunks = 1,
    };
}

/**
 * The stored bytes of chunk (it, iz, iy, ix). Each shard holds one
 * uncompressed chunk, then an index of (offset, nbytes) and a checksum.
 */
std::vector<uint8_t>
read_chunk(size_t it, size_t iz, size_t iy, size_t ix)
{
    constexpr size_t index_bytes = 2 * sizeof(uint64_t) + sizeof(uint32_t);
    const fs::path path = fs::path(data_path) / "c" / std::to_string(it) /
                          std::to_string(iz) / std::to_string(iy) /
                          std::to_string(ix);
    EXPECT(fs::is_regular_file(path), "missing shard ", path.string());

    std::ifstream file(path, std::ios::binary);
    std::vector<uint8_t> shard(fs::file_size(path));
    file.read(reinterpret_cast<char*>(shard.data()), shard.size());
    CHECK(file.good());
    EXPECT_EQ(size_t, shard.size(), chunk_bytes + index_bytes);

    // every chunk holds some frame data, so none is skipped as all-zero
    uint64_t index[2];
    memcpy(index, shard.data() + chunk_bytes, sizeof(index));
    EXPECT_EQ(size_t, index[0], 0);
    EXPECT_EQ(size_t, index[1], chunk_bytes);

    return { shard.begin(), shard.begin() + chunk_bytes };
}

/** Count the bytes of chunk (it, iz, iy, ix) that differ from the frames. */
size_t
count_mismatches(size_t it, size_t iz, size_t iy, size_t ix)
{
    const auto chunk = read_chunk(it, iz, iy, ix);

    size_t n = 0, offset = 0;
    for (size_t tt = 0; tt < chunk_t; ++tt) {
        const size_t t = it * chunk_t + tt;
        for (size_t zz = 0; zz < chunk_z; ++zz) {
            const size_t z = iz * chunk_z + zz;
            for (size_t yy = 0; yy < chunk_y; ++yy) {
                const size_t y = iy * chunk_y + yy;
                for (size_t xx = 0; xx < chunk_x; ++xx, ++offset) {
                    const size_t x = ix * chunk_x + xx;
                    const bool in_array = t < n_timepoints && z < n_planes &&
                                          y < height && x < width;
                    const uint8_t expected =
                      in_array ? frame_value(t, z) : uint8_t(0);
                    n += chunk[offset] != expected;
                }
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

        std::vector<uint8_t> frame(frame_bytes);
        for (size_t t = 0; t < n_timepoints; ++t) {
            for (size_t z = 0; z < n_planes; ++z) {
                std::fill(frame.begin(), frame.end(), frame_value(t, z));
                size_t bytes_out = 0;
                EXPECT_EQ(
                  int,
                  ZarrStream_append(
                    stream, frame.data(), frame_bytes, &bytes_out, nullptr),
                  ZarrStatusCode_Success);
                EXPECT_EQ(size_t, bytes_out, frame_bytes);
            }
        }

        EXPECT_EQ(int, ZarrStream_close(stream), ZarrStatusCode_Success);
        stream = nullptr;

        size_t mismatches = 0, bad_chunks = 0;
        for (size_t it = 0; it < chunks_t; ++it) {
            for (size_t iz = 0; iz < chunks_z; ++iz) {
                for (size_t iy = 0; iy < chunks_y; ++iy) {
                    for (size_t ix = 0; ix < chunks_x; ++ix) {
                        const size_t n = count_mismatches(it, iz, iy, ix);
                        if (n) {
                            LOG_ERROR("chunk (",
                                      it,
                                      ", ",
                                      iz,
                                      ", ",
                                      iy,
                                      ", ",
                                      ix,
                                      "): ",
                                      n,
                                      " bytes differ");
                            mismatches += n;
                            ++bad_chunks;
                        }
                    }
                }
            }
        }

        if (mismatches) {
            LOG_ERROR(mismatches, " bytes differ in ", bad_chunks, " chunks");
        }
        CHECK(mismatches == 0);
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
