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

constexpr unsigned int array_width = 32, array_height = 24, array_channels = 2;
constexpr size_t nbytes_px = sizeof(uint16_t);
constexpr size_t bytes_of_frame = array_width * array_height * nbytes_px;

// A single-resolution (non-multiscale) image carrying omero rendering metadata.
// Presence of omero makes it an OME image group, so it gains a multiscales
// block with a single dataset plus the omero block.
ZarrStream*
setup(const ZarrOMEVersion ome_version,
      ZarrOMERenderingSettings* omero,
      const bool multiscale)
{
    ZarrArraySettings array = {
        .data_type = ZarrDataType_uint16,
        .multiscale = multiscale,
        .downsampling_method = ZarrDownsamplingMethod_Mean,
        .omero = omero,
    };
    ZarrStreamSettings settings = {
        .store_path = test_path.c_str(),
        .s3_settings = nullptr,
        .max_threads = 0,
        .arrays = &array,
        .array_count = 1,
        .ome_version = ome_version,
    };

    CHECK_OK(ZarrArraySettings_create_dimension_array(settings.arrays, 3));

    ZarrDimensionProperties* dim = settings.arrays->dimensions;
    *dim = DIM("c",
               ZarrDimensionType_Channel,
               array_channels,
               1,
               array_channels,
               nullptr,
               1.0);

    dim = settings.arrays->dimensions + 1;
    *dim = DIM("y",
               ZarrDimensionType_Space,
               array_height,
               array_height,
               1,
               "micrometer",
               0.9);

    dim = settings.arrays->dimensions + 2;
    *dim = DIM("x",
               ZarrDimensionType_Space,
               array_width,
               array_width,
               1,
               "micrometer",
               0.9);

    auto* stream = ZarrStream_create(&settings);
    ZarrArraySettings_destroy_dimension_array(settings.arrays);

    return stream;
}

nlohmann::json
read_group_ome(const std::string& path)
{
    const fs::path group_metadata_path = fs::path(path) / "zarr.json";
    EXPECT(fs::is_regular_file(group_metadata_path),
           "Expected file '",
           group_metadata_path.string(),
           "' to exist");
    std::ifstream f(group_metadata_path);
    nlohmann::json meta = nlohmann::json::parse(f);
    return meta["attributes"]["ome"];
}

std::string
ome_version_to_str(const ZarrOMEVersion version)
{
    switch (version) {
        case ZarrOMEVersion_0_5:
            return "0.5";
        case ZarrOMEVersion_0_6:
            return "0.6";
        default:
            throw std::runtime_error("Unexpected OME version");
    }
}

void
run(ZarrOMEVersion ome_version,
    ZarrOMERenderingSettings* omero,
    bool multiscale)
{
    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    auto* stream = setup(ome_version, omero, multiscale);
    CHECK(stream != nullptr);

    std::vector<uint16_t> frame(array_width * array_height, 0);
    size_t bytes_out;
    for (auto i = 0; i < array_channels; ++i) {
        ZarrStatusCode status = ZarrStream_append(
          stream, frame.data(), bytes_of_frame, &bytes_out, nullptr);
        EXPECT(status == ZarrStatusCode_Success,
               "Failed to append frame ",
               i,
               ": ",
               Zarr_get_status_message(status));
    }
    ZarrStream_destroy(stream);

    const auto ome = read_group_ome(test_path);

    const auto expected_version = ome_version_to_str(ome_version);
    const auto actual_version = ome["version"].get<std::string>();
    EXPECT(actual_version == expected_version,
           "Expected version '",
           expected_version,
           "', got '",
           actual_version,
           "'");

    // an OME image group always has multiscales
    EXPECT(ome.contains("multiscales"), "Expected multiscales in ome metadata");

    if (omero != nullptr) {
        EXPECT(ome.contains("omero"), "Expected omero in ome metadata");
        const auto& j = ome["omero"];

        EXPECT_STR_EQ(j["name"].get<std::string>().c_str(), "test image");

        const auto& channels = j["channels"];
        EXPECT_EQ(size_t, channels.size(), 2);

        EXPECT_STR_EQ(channels[0]["label"].get<std::string>().c_str(), "red");
        EXPECT_STR_EQ(channels[0]["color"].get<std::string>().c_str(),
                      "FF0000");
        EXPECT(channels[0]["active"].get<bool>(), "Expected channel 0 active");
        EXPECT_EQ(int, channels[0]["window"]["max"].get<double>(), 65535.0);
        EXPECT_EQ(int, channels[0]["window"]["end"].get<double>(), 1500.0);
        // has_coefficient was false -> field omitted
        EXPECT(!channels[0].contains("coefficient"),
               "Expected coefficient to be omitted for channel 0");

        EXPECT_STR_EQ(channels[1]["label"].get<std::string>().c_str(), "green");
        EXPECT_EQ(int, channels[1]["coefficient"].get<double>(), 1.0);

        // rdefs present
        EXPECT(j.contains("rdefs"), "Expected rdefs in omero metadata");
        EXPECT_STR_EQ(j["rdefs"]["model"].get<std::string>().c_str(), "color");
    } else {
        EXPECT(!ome.contains("omero"),
               "Expected no omero block when none configured");
    }
}

// Settings that should not survive validation.
void
expect_rejected(ZarrOMERenderingSettings* omero, const char* why)
{
    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    auto* stream = setup(ZarrOMEVersion_0_5, omero, /*multiscale=*/false);
    EXPECT(stream == nullptr, "Expected rejection: ", why);
}
} // namespace

int
main()
{
    Zarr_set_log_level(ZarrLogLevel_Debug);

    int retval = 1;
    try {
        // omero channels
        ZarrOMEChannel channels[2] = {};
        channels[0].label = "red";
        channels[0].color = "FF0000";
        channels[0].window = {
            .min = 0.0, .max = 65535.0, .start = 0.0, .end = 1500.0
        };
        channels[0].active = true;

        channels[1].label = "green";
        channels[1].color = "00FF00";
        channels[1].window = {
            .min = 0.0, .max = 65535.0, .start = 0.0, .end = 2000.0
        };
        channels[1].active = true;
        channels[1].coefficient = 1.0;
        channels[1].has_coefficient = true;

        ZarrOMERenderingSettings omero = {};
        omero.name = "test image";
        omero.channels = channels;
        omero.channel_count = 2;
        omero.has_rdefs = true;
        omero.rdefs.model = "color";

        // default version (0.5); omero alone makes it an OME image group
        run(ZarrOMEVersion_0_5, &omero, /*multiscale=*/false);

        // opt in to 0.6 via a multiscale image, no omero
        run(ZarrOMEVersion_0_6, nullptr, /*multiscale=*/true);

        // an empty omero block would write "channels": [], which is invalid
        ZarrOMERenderingSettings empty = {};
        expect_rejected(&empty, "omero with no channels");

        // one channel too few for the 2-wide Channel dimension
        ZarrOMERenderingSettings short_channels = omero;
        short_channels.channel_count = 1;
        expect_rejected(&short_channels, "omero channel count mismatch");

        // a zero-initialized window renders the channel blank
        ZarrOMEChannel blank_window[2] = { channels[0], channels[1] };
        blank_window[1].window = {};
        ZarrOMERenderingSettings blank = omero;
        blank.channels = blank_window;
        expect_rejected(&blank, "omero channel with a zeroed window");

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
    }

    if (fs::exists(test_path)) {
        fs::remove_all(test_path);
    }

    return retval;
}