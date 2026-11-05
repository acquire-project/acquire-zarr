#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

namespace {
const std::string test_path =
  (fs::temp_directory_path() / (TEST ".zarr")).string();

// the appended dimension is spatial, so it is the z of a 3D pyramid
constexpr uint32_t frames = 8, array_yx = 128, chunk_yx = 32, chunk_z = 4;
constexpr double scale_z = 2.0, scale_yx = 0.5;

nlohmann::json
read_json(const fs::path& path)
{
    std::ifstream f(path);
    EXPECT(f.is_open(), "Failed to open ", path.string());
    return nlohmann::json::parse(f);
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
    CHECK_OK(ZarrArraySettings_create_dimension_array(&array, 3));
    array.dimensions[0] =
      DIM("z", ZarrDimensionType_Space, 0, chunk_z, 1, "micrometer", scale_z);
    array.dimensions[1] = DIM("y",
                              ZarrDimensionType_Space,
                              array_yx,
                              chunk_yx,
                              1,
                              "micrometer",
                              scale_yx);
    array.dimensions[2] = DIM("x",
                              ZarrDimensionType_Space,
                              array_yx,
                              chunk_yx,
                              1,
                              "micrometer",
                              scale_yx);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(&array);
    CHECK(stream);

    std::vector<uint16_t> frame(array_yx * array_yx);
    for (uint32_t i = 0; i < frames; ++i) {
        for (size_t j = 0; j < frame.size(); ++j) {
            frame[j] = static_cast<uint16_t>(i * 100 + j % 97);
        }
        size_t bytes_out;
        CHECK_OK(ZarrStream_append(stream,
                                   frame.data(),
                                   frame.size() * sizeof(uint16_t),
                                   &bytes_out,
                                   nullptr));
    }
    ZarrStream_destroy(stream);
}

void
verify_scales()
{
    const auto group = read_json(fs::path(test_path) / "zarr.json");
    const auto& datasets =
      group["attributes"]["ome"]["multiscales"][0]["datasets"];
    EXPECT(datasets.size() > 1, "Expected a pyramid, got one level");

    for (size_t level = 0; level < datasets.size(); ++level) {
        const auto& scale =
          datasets[level]["coordinateTransformations"][0]["scale"];
        EXPECT_EQ(size_t, scale.size(), 3);
        EXPECT_EQ(double, scale[0].get<double>(), scale_z);
        EXPECT_EQ(double, scale[1].get<double>(), scale_yx * (1 << level));
        EXPECT_EQ(double, scale[2].get<double>(), scale_yx * (1 << level));

        const auto meta =
          read_json(fs::path(test_path) / std::to_string(level) / "zarr.json");
        EXPECT_EQ(uint32_t, meta["shape"][0].get<uint32_t>(), frames);
        EXPECT_EQ(
          uint32_t, meta["shape"][1].get<uint32_t>(), array_yx >> level);
        EXPECT_EQ(
          uint32_t, meta["shape"][2].get<uint32_t>(), array_yx >> level);
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
        verify_scales();
        fs::remove_all(test_path);
        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }
    return retval;
}
