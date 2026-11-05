#include "acquire.zarr.h"
#include "test.macros.hh"
#include "hcs-test-helper.hh"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <unordered_set>

namespace fs = std::filesystem;

ZarrStream*
make_hcs_stream()
{
    ZarrArraySettings fields[hcs::field_count];
    hcs::configure_fields(fields);

    ZarrStreamSettings settings = {
        .store_path = TEST ".zarr",
        .overwrite = true,
        .arrays = fields,
        .array_count = hcs::field_count,
    };

    size_t n_arrays = ZarrStreamSettings_get_array_count(&settings);
    EXPECT(n_arrays == 3,
           "Expected 3 arrays in the stream settings, got ",
           n_arrays);

    std::vector<std::string> array_keys;
    char* c_key = nullptr;
    for (size_t i = 0; i < n_arrays; ++i) {
        if (auto code = ZarrStreamSettings_get_array_key(&settings, i, &c_key);
            code != ZarrStatusCode_Success) {
            std::string error = "Failed to get array paths: " +
                                std::string(Zarr_get_status_message(code));
            throw std::runtime_error(error);
        }

        array_keys.emplace_back(c_key);
        free(c_key);
    }

    const std::unordered_set<std::string> expected_paths = {
        "test_plate/C/5/fov1",
        "test_plate/C/5/fov2",
        "test_plate/D/7/fov1",
    };

    for (size_t i = 0; i < n_arrays; ++i) {
        const auto& key(array_keys[i]);
        EXPECT(expected_paths.contains(key), "Unexpected array path: ", key);
    }

    ZarrStream* stream = ZarrStream_create(&settings);
    hcs::destroy_fields(fields);
    EXPECT(stream != nullptr, "Failed to create stream");

    hcs::write_metadata(stream);

    return stream;
}

void
append_some_data(ZarrStream* stream)
{
    size_t bytes_out = 0;
    std::vector<uint8_t> data;

    // write to FOV1 in well C/5
    data.resize(640 * 480);
    CHECK(
      ZarrStream_append(
        stream, data.data(), data.size(), &bytes_out, "test_plate/C/5/fov1") ==
      ZarrStatusCode_Success);
    EXPECT(bytes_out == data.size(),
           "Expected to write ",
           data.size(),
           " bytes, wrote ",
           bytes_out);

    // write to FOV2 in well C/5
    data.resize(640 * 480 * 2);
    CHECK(
      ZarrStream_append(
        stream, data.data(), data.size(), &bytes_out, "test_plate/C/5/fov2") ==
      ZarrStatusCode_Success);
    EXPECT(bytes_out == data.size(),
           "Expected to write ",
           data.size(),
           " bytes, wrote ",
           bytes_out);

    // write to FOV1 in well D/7
    data.resize(512 * 512 * 2);
    CHECK(
      ZarrStream_append(
        stream, data.data(), data.size(), &bytes_out, "test_plate/D/7/fov1") ==
      ZarrStatusCode_Success);
    EXPECT(bytes_out == data.size(),
           "Expected to write ",
           data.size(),
           " bytes, wrote ",
           bytes_out);
}

void
validate_generic_group_metadata()
{
    const fs::path base_path = TEST ".zarr";
    std::vector<fs::path> paths = {
        base_path / "zarr.json",
        base_path / "test_plate" / "C" / "zarr.json",
        base_path / "test_plate" / "D" / "zarr.json",
    };

    for (const auto& path : paths) {
        EXPECT(fs::exists(path), "Missing metadata file: ", path.string());

        std::ifstream ifs(path);
        CHECK(ifs.is_open());
        nlohmann::json metadata;
        ifs >> metadata;
        ifs.close();

        CHECK(metadata.contains("zarr_format"));
        CHECK(metadata["zarr_format"] == 3);

        CHECK(metadata.contains("consolidated_metadata"));
        CHECK(metadata["consolidated_metadata"].is_null());

        CHECK(metadata.contains("node_type"));
        CHECK(metadata["node_type"] == "group");

        CHECK(metadata.contains("attributes"));
        CHECK(metadata["attributes"].empty());
    }
}

void
validate_plate_metadata()
{
    const fs::path base_path = TEST ".zarr";
    const fs::path plate_path = base_path / "test_plate" / "zarr.json";

    CHECK(fs::exists(plate_path));

    std::ifstream ifs(plate_path);
    CHECK(ifs.is_open());
    nlohmann::json metadata;
    ifs >> metadata;
    ifs.close();

    CHECK(metadata.contains("zarr_format"));
    CHECK(metadata["zarr_format"] == 3);

    CHECK(metadata.contains("consolidated_metadata"));
    CHECK(metadata["consolidated_metadata"].is_null());

    CHECK(metadata.contains("node_type"));
    CHECK(metadata["node_type"] == "group");

    CHECK(metadata.contains("attributes"));
    const auto& attributes = metadata["attributes"];

    CHECK(attributes.contains("ome"));
    const auto& ome = attributes["ome"];
    CHECK(ome.size() == 2);

    CHECK(ome.contains("version"));
    CHECK(ome["version"] == "0.5");

    CHECK(ome.contains("plate"));
    const auto& plate = ome["plate"];

    EXPECT(plate.size() == 6, "Expected 6 fields in plate, got ", plate.size());

    // plate field 1: name
    CHECK(plate.contains("name"));
    EXPECT(plate["name"] == "Test Plate",
           "Expected plate name to be 'Test Plate', got ",
           plate["name"].get<std::string>());

    // plate field 3: field_count
    CHECK(plate.contains("field_count"));
    EXPECT(plate["field_count"].get<int>() == 2,
           "Expected plate field_count to be 2, got ",
           plate["field_count"].get<int>());

    // plate field 4: acquisitions
    CHECK(plate.contains("acquisitions"));
    CHECK(plate["acquisitions"].is_array());
    const auto& acquisitions = plate["acquisitions"];
    EXPECT(acquisitions.size() == 2,
           "Expected 2 acquisitions, got ",
           acquisitions.size());

    EXPECT(acquisitions[0].size() == 4,
           "Expected 4 fields in acquisition, got ",
           acquisitions[0].size());

    CHECK(acquisitions[0].contains("id"));
    EXPECT(acquisitions[0]["id"].get<int>() == 0,
           "Expected acquisition id to be 0, got ",
           acquisitions[0]["id"].get<int>());

    CHECK(acquisitions[0].contains("maximumfieldcount"));
    EXPECT(acquisitions[0]["maximumfieldcount"].get<int>() == 1,
           "Expected maximumfieldcount to be 1, got ",
           acquisitions[0]["maximumfieldcount"].get<int>());

    CHECK(acquisitions[0].contains("name"));
    EXPECT(acquisitions[0]["name"] == "Meas_01(2012-07-31_10-41-12)",
           "Expected acquisition name to be Meas_01(2012-07-31_10-41-12), got ",
           acquisitions[0]["name"].get<std::string>());

    CHECK(!acquisitions[0].contains("description"));

    CHECK(acquisitions[0].contains("starttime"));
    EXPECT(acquisitions[0]["starttime"].get<uint64_t>() == 1343731272000ULL,
           "Expected starttime to be 1343731272000, got ",
           acquisitions[0]["starttime"].get<uint64_t>());

    CHECK(!acquisitions[0].contains("endtime"));

    EXPECT(acquisitions[1].size() == 5,
           "Expected 5 fields in acquisition, got ",
           acquisitions[1].size());

    CHECK(acquisitions[1].contains("id"));
    EXPECT(acquisitions[1]["id"].get<int>() == 1,
           "Expected acquisition id to be 1, got ",
           acquisitions[1]["id"].get<int>());

    CHECK(acquisitions[1].contains("maximumfieldcount"));
    EXPECT(acquisitions[1]["maximumfieldcount"].get<int>() == 1,
           "Expected maximumfieldcount to be 1, got ",
           acquisitions[1]["maximumfieldcount"].get<int>());

    CHECK(acquisitions[1].contains("name"));
    EXPECT(acquisitions[1]["name"] == "Meas_02(2012-07-31_10-45-12)",
           "Expected acquisition name to be Meas_02(2012-07-31_10-45-12), got ",
           acquisitions[1]["name"].get<std::string>());

    CHECK(!acquisitions[1].contains("description"));

    CHECK(acquisitions[1].contains("starttime"));
    EXPECT(acquisitions[1]["starttime"].get<uint64_t>() == 1343735801000ULL,
           "Expected starttime to be 1343735801000, got ",
           acquisitions[1]["starttime"].get<uint64_t>());

    CHECK(acquisitions[1].contains("endtime"));
    EXPECT(acquisitions[1]["endtime"].get<uint64_t>() == 1343737645000ULL,
           "Expected endtime to be 1343737645000, got ",
           acquisitions[1]["endtime"].get<uint64_t>());

    // plate field 5: columns
    CHECK(plate.contains("columns"));
    CHECK(plate["columns"].is_array());
    const auto& columns = plate["columns"];
    EXPECT(columns.size() == 12, "Expected 12 columns, got ", columns.size());
    for (auto i = 1; i <= 12; ++i) {
        EXPECT(columns[i - 1]["name"].get<std::string>() == std::to_string(i),
               "Expected column name to be ",
               i,
               ", got ",
               columns[i - 1]["name"].get<std::string>());
    }

    // plate field 6: rows
    CHECK(plate.contains("rows"));
    CHECK(plate["rows"].is_array());
    const auto& rows = plate["rows"];
    EXPECT(rows.size() == 8, "Expected 8 rows, got ", rows.size());
    for (char r = 'A'; r <= 'H'; ++r) {
        EXPECT(rows[r - 'A']["name"].get<std::string>() == std::string(1, r),
               "Expected row name to be ",
               r,
               ", got ",
               rows[r - 'A']["name"].get<std::string>());
    }

    // plate field 7: wells
    CHECK(plate.contains("wells"));
    CHECK(plate["wells"].is_array());
    const auto& wells = plate["wells"];
    EXPECT(wells.size() == 2, "Expected 2 wells, got ", wells.size());

    CHECK(wells[0].size() == 3);
    CHECK(wells[0].contains("rowIndex"));
    EXPECT(wells[0]["rowIndex"].get<int>() == 2,
           "Expected rowIndex to be 2, got ",
           wells[0]["rowIndex"].get<int>());
    CHECK(wells[0].contains("columnIndex"));
    EXPECT(wells[0]["columnIndex"].get<int>() == 4,
           "Expected columnIndex to be 4, got ",
           wells[0]["columnIndex"].get<int>());
    CHECK(wells[0].contains("path"));
    EXPECT(wells[0]["path"].get<std::string>() == "C/5",
           "Expected path to be C/5, got ",
           wells[0]["path"].get<std::string>());

    CHECK(wells[1].size() == 3);
    CHECK(wells[1].contains("rowIndex"));
    EXPECT(wells[1]["rowIndex"].get<int>() == 3,
           "Expected rowIndex to be 3, got ",
           wells[1]["rowIndex"].get<int>());
    CHECK(wells[1].contains("columnIndex"));
    EXPECT(wells[1]["columnIndex"].get<int>() == 6,
           "Expected columnIndex to be 6, got ",
           wells[1]["columnIndex"].get<int>());
    CHECK(wells[1].contains("path"));
    EXPECT(wells[1]["path"].get<std::string>() == "D/7",
           "Expected path to be D/7, got ",
           wells[1]["path"].get<std::string>());
}

void
validate_well_metadata()
{
    const fs::path base_path = TEST ".zarr";
    std::vector<fs::path> paths = {
        base_path / "test_plate" / "C" / "5" / "zarr.json",
        base_path / "test_plate" / "D" / "7" / "zarr.json",
    };
    std::vector<int> expected_image_counts = { 2, 1 };

    for (auto i = 0; i < paths.size(); ++i) {
        const auto& path = paths[i];
        std::ifstream ifs(path);
        CHECK(ifs.is_open());
        nlohmann::json metadata;
        ifs >> metadata;
        ifs.close();

        CHECK(metadata.contains("zarr_format"));
        CHECK(metadata["zarr_format"] == 3);

        CHECK(metadata.contains("consolidated_metadata"));
        CHECK(metadata["consolidated_metadata"].is_null());

        CHECK(metadata.contains("node_type"));
        CHECK(metadata["node_type"] == "group");

        CHECK(metadata.contains("attributes"));
        auto& attributes = metadata["attributes"];

        CHECK(attributes.contains("ome"));
        const auto& ome = attributes["ome"];
        CHECK(ome.size() == 2);

        CHECK(ome.contains("version"));
        CHECK(ome["version"] == "0.5");

        CHECK(ome.contains("well"));
        const auto& well = ome["well"];
        EXPECT(well.size() == 1, "Expected 1 field in well, got ", well.size());

        CHECK(well.contains("images"));
        const auto& images = well["images"];
        EXPECT(images.size() == expected_image_counts[i],
               "Expected ",
               expected_image_counts[i],
               " images, got ",
               images.size());

        EXPECT(images[0].size() == 2,
               "Expected 2 fields in image, got ",
               images[0].size());
        CHECK(images[0].contains("acquisition"));
        EXPECT(images[0]["acquisition"].get<int>() == 0,
               "Expected acquisition to be 0, got ",
               images[0]["acquisition"].get<int>());
        CHECK(images[0].contains("path"));
        EXPECT(images[0]["path"].get<std::string>() == "fov1",
               "Expected path to be fov1, got ",
               images[0]["path"].get<std::string>());

        if (expected_image_counts[i] == 2) {
            CHECK(images[1].size() == 2);
            CHECK(images[1].contains("acquisition"));
            EXPECT(images[1]["acquisition"].get<int>() == 1,
                   "Expected acquisition to be 1, got ",
                   images[1]["acquisition"].get<int>());
            CHECK(images[1].contains("path"));
            EXPECT(images[1]["path"].get<std::string>() == "fov2",
                   "Expected path to be fov2, got ",
                   images[1]["path"].get<std::string>());
        }
    }
}

void
check_arrays_exist()
{
    const fs::path base_path = TEST ".zarr";
    std::vector<fs::path> paths = {
        base_path / "test_plate" / "C" / "5" / "fov1",
        base_path / "test_plate" / "C" / "5" / "fov2",
        base_path / "test_plate" / "D" / "7" / "fov1",
    };

    for (const auto& path : paths) {
        EXPECT(
          fs::exists(path / "zarr.json"), "Missing array: ", path.string());

        // is_ngff makes each field of view a multiscales group, with its
        // full-resolution array at level 0
        std::ifstream ifs(path / "zarr.json");
        nlohmann::json metadata;
        ifs >> metadata;
        EXPECT(metadata["node_type"] == "group",
               "Expected a group at ",
               path.string());
        EXPECT(metadata["attributes"]["ome"].contains("multiscales"),
               "Expected multiscales at ",
               path.string());
        EXPECT(fs::exists(path / "0" / "zarr.json"),
               "Missing level 0 at ",
               path.string());
    }
}

int
main()
{
    int retval = 1;

    try {
        ZarrStream* stream = make_hcs_stream();

        // plate and well metadata are written at once, so a reader can open
        // the plate during the acquisition
        validate_plate_metadata();
        validate_well_metadata();

        append_some_data(stream);
        ZarrStream_destroy(stream);

        validate_generic_group_metadata();
        validate_plate_metadata();
        validate_well_metadata();
        check_arrays_exist();

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed: ", exc.what());
    }

    if (fs::exists(TEST ".zarr")) {
        fs::remove_all(TEST ".zarr");
    }

    return retval;
}