#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();

// a 2D array: (y, x) only, so ArrayDimensions adds a phantom dimension
constexpr uint32_t array_y = 256, array_x = 256, chunk = 64;

uint16_t
sample(uint32_t y, uint32_t x)
{
    return static_cast<uint16_t>((y * 37 + x * 11) % 4096);
}

nlohmann::json
read_json(const fs::path& path)
{
    std::ifstream f(path);
    EXPECT(f.is_open(), "Failed to open ", path.string());
    return nlohmann::json::parse(f);
}

std::vector<uint8_t>
read_single_chunk_shard(const fs::path& path)
{
    std::ifstream f(path, std::ios::binary);
    EXPECT(f.is_open(), "Failed to open ", path.string());
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(f)),
                               std::istreambuf_iterator<char>());

    // index at the end: (offset, nbytes) as uint64, then a crc32c
    const size_t index_bytes = 2 * sizeof(uint64_t) + 4;
    EXPECT(bytes.size() > index_bytes, "Shard too small: ", path.string());
    uint64_t offset, nbytes;
    std::memcpy(&offset, bytes.data() + bytes.size() - index_bytes, 8);
    std::memcpy(&nbytes, bytes.data() + bytes.size() - index_bytes + 8, 8);
    EXPECT(offset + nbytes <= bytes.size() - index_bytes, "Bad shard index");
    return { bytes.begin() + offset, bytes.begin() + offset + nbytes };
}

void
write_store()
{
    ZarrArraySettings array = {
        .data_type = ZarrDataType_uint16,
        .downsampling_method = ZarrDownsamplingMethod_Mean,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .max_threads = 0,
        .arrays = &array,
        .array_count = 1,
    };
    CHECK_OK(ZarrArraySettings_create_dimension_array(&array, 2));
    array.dimensions[0] =
      DIM("y", ZarrDimensionType_Space, array_y, chunk, 1, "micrometer", 1.0);
    array.dimensions[1] =
      DIM("x", ZarrDimensionType_Space, array_x, chunk, 1, "micrometer", 1.0);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(&array);
    CHECK(stream);

    std::vector<uint16_t> frame(array_y * array_x);
    for (uint32_t y = 0; y < array_y; ++y) {
        for (uint32_t x = 0; x < array_x; ++x) {
            frame[y * array_x + x] = sample(y, x);
        }
    }
    size_t bytes_out;
    CHECK_OK(ZarrStream_append(stream,
                               frame.data(),
                               frame.size() * sizeof(uint16_t),
                               &bytes_out,
                               nullptr));
    ZarrStream_destroy(stream);
}

void
verify_levels()
{
    const auto group = read_json(fs::path(test_path) / "zarr.json");
    const auto& multiscale = group["attributes"]["ome"]["multiscales"][0];
    EXPECT_EQ(size_t, multiscale["axes"].size(), 2);

    const auto& datasets = multiscale["datasets"];
    EXPECT(datasets.size() > 1, "Expected a pyramid, got one level");

    for (size_t level = 0; level < datasets.size(); ++level) {
        const auto meta =
          read_json(fs::path(test_path) / std::to_string(level) / "zarr.json");
        EXPECT_EQ(size_t, meta["shape"].size(), 2);
        EXPECT(meta["dimension_names"] == nlohmann::json({ "y", "x" }),
               "Level ",
               level,
               " has dimension_names ",
               meta["dimension_names"].dump());
        EXPECT_EQ(
          size_t, meta["chunk_grid"]["configuration"]["chunk_shape"].size(), 2);
        EXPECT_EQ(
          size_t, meta["codecs"][0]["configuration"]["chunk_shape"].size(), 2);
        EXPECT_EQ(
          size_t,
          datasets[level]["coordinateTransformations"][0]["scale"].size(),
          2);
    }

    // every chunk of level 1: the 2x2 mean of the input, truncated
    constexpr uint32_t level1_chunks = array_y / 2 / chunk;
    for (uint32_t cy = 0; cy < level1_chunks; ++cy) {
        for (uint32_t cx = 0; cx < level1_chunks; ++cx) {
            const auto bytes =
              read_single_chunk_shard(fs::path(test_path) / "1" / "c" /
                                      std::to_string(cy) / std::to_string(cx));
            EXPECT_EQ(size_t, bytes.size(), chunk * chunk * sizeof(uint16_t));
            std::vector<uint16_t> level1(chunk * chunk);
            std::memcpy(level1.data(), bytes.data(), bytes.size());
            for (uint32_t y = 0; y < chunk; ++y) {
                for (uint32_t x = 0; x < chunk; ++x) {
                    const uint32_t y0 = 2 * (cy * chunk + y);
                    const uint32_t x0 = 2 * (cx * chunk + x);
                    const uint32_t sum = sample(y0, x0) + sample(y0, x0 + 1) +
                                         sample(y0 + 1, x0) +
                                         sample(y0 + 1, x0 + 1);
                    EXPECT_EQ(int, level1[y * chunk + x], sum / 4);
                }
            }
        }
    }
}
} // namespace

int
main()
{
    int retval = 1;
    try {
        fs::remove_all(test_path); // a failed run leaves its store behind
        write_store();
        verify_levels();
        fs::remove_all(test_path);
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }
    return retval;
}
