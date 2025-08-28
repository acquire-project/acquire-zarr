#include "plate.hh"
#include "unit.test.macros.hh"

void
check_dense_plate()
{
    std::string path = "/data/plate";
    std::string name = "Test Plate";

    std::vector<zarr::Well> wells;
    wells.emplace_back(zarr::Well{
      .row_name = "A",
      .column_name = "1",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" },
                  zarr::FieldOfView{ .acquisition_id = 1, .path = "fov2" } } });

    wells.emplace_back(zarr::Well{
      .row_name = "A",
      .column_name = "2",
      .images = {
        zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1-1" },
        zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1-2" },
        zarr::FieldOfView{ .acquisition_id = 2, .path = "fov2-1" },
        zarr::FieldOfView{ .acquisition_id = 2, .path = "fov2-2" } } });

    wells.emplace_back(zarr::Well{
      .row_name = "A",
      .column_name = "3",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" } } });

    wells.emplace_back(zarr::Well{
      .row_name = "B",
      .column_name = "1",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" } } });

    wells.emplace_back(zarr::Well{
      .row_name = "B",
      .column_name = "2",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" } } });

    wells.emplace_back(zarr::Well{
      .row_name = "B",
      .column_name = "3",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" },
                  zarr::FieldOfView{ .acquisition_id = 1, .path = "fov2" },
                  zarr::FieldOfView{ .acquisition_id = 1, .path = "fov3" } } });

    std::vector<zarr::Acquisition> acquisitions{
        zarr::Acquisition{ .id = 1, .name = "Acquisition 0" },
        zarr::Acquisition{ .id = 2, .name = "Acquisition 1" }
    };

    zarr::Plate plate(
      path, name, { "A", "B" }, { "1", "2", "3" }, wells, acquisitions);

    EXPECT(plate.path() == path,
           "Plate path mismatch: Expected ",
           path,
           " got ",
           plate.path());

    EXPECT(plate.name() == name,
           "Plate name mismatch: Expected ",
           name,
           " got ",
           plate.name());

    const auto& rows = plate.row_names();
    EXPECT(rows.size() == 2,
           "Expected 2 rows, got " + std::to_string(rows.size()));
    EXPECT(rows[0] == "A", "Expected row 0 to be 'A', got '" + rows[0] + "'");
    EXPECT(rows[1] == "B", "Expected row 1 to be 'B', got '" + rows[1] + "'");

    const auto& columns = plate.column_names();
    EXPECT(columns.size() == 3,
           "Expected 3 columns, got " + std::to_string(columns.size()));
    EXPECT(columns[0] == "1",
           "Expected column 0 to be '1', got '" + columns[0] + "'");
    EXPECT(columns[1] == "2",
           "Expected column 1 to be '2', got '" + columns[1] + "'");
    EXPECT(columns[2] == "3",
           "Expected column 2 to be '3', got '" + columns[2] + "'");

    EXPECT(plate.field_count() == 4,
           "Expected field count to be 4, got ",
           plate.field_count());
    EXPECT(plate.maximum_field_count(1) == 3,
           "Expected max field count for Acquisition 0 to be 3, got ",
           plate.maximum_field_count(1));
    EXPECT(plate.maximum_field_count(2) == 2,
           "Expected max field count for acquisition 2 to be 2, got ",
           plate.maximum_field_count(2));

    // there should be two acquisitions
    EXPECT(plate.acquisitions().has_value(), "Expected some acquisitions");
    std::vector<zarr::Acquisition> test_acquisitions = *plate.acquisitions();

    EXPECT(test_acquisitions.size() == 2,
           "Expected 2 acquisitions, got ",
           test_acquisitions.size());

    EXPECT(test_acquisitions[0].id == 1,
           "Expected acquisition 0 id to be 1, got ",
           test_acquisitions[0].id);
    EXPECT(test_acquisitions[0].name.has_value(),
           "Expected acquisition 0 to have a name");
    EXPECT(test_acquisitions[0].name.value() == "Acquisition 0",
           "Expected acquisition 0 name to be 'Acquisition 0', got '",
           test_acquisitions[0].name.value());

    EXPECT(test_acquisitions[1].id == 2,
           "Expected acquisition 1 id to be 2, got ",
           test_acquisitions[1].id);
    EXPECT(test_acquisitions[1].name.has_value(),
           "Expected acquisition 1 to have a name");
    EXPECT(test_acquisitions[1].name.value() == "Acquisition 1",
           "Expected acquisition 1 name to be 'Acquisition 1', got '",
           test_acquisitions[1].name.value());

    auto json = plate.to_json();

    EXPECT(json.contains("name"), "Plate JSON missing 'name' key");
    EXPECT(json["name"] == name,
           "Plate JSON 'name' key mismatch: Expected ",
           name,
           " got ",
           json["name"].get<std::string>());

    EXPECT(json.contains("field_count"),
           "Plate JSON missing 'field_count' key");
    EXPECT(json["field_count"] == 4,
           "Plate JSON 'field_count' key mismatch: Expected 4 got ",
           json["field_count"].get<uint32_t>());

    EXPECT(json.contains("rows"), "Plate JSON missing 'rows' key");
    EXPECT(json["rows"].is_array(), "Plate JSON 'rows' key is not an array");
    EXPECT(json["rows"].size() == 2,
           "Plate JSON 'rows' array size mismatch: Expected 2 got ",
           json["rows"].size());

    EXPECT(json["rows"][0].contains("name"),
           "Plate JSON 'rows[0]' missing 'name' key");
    EXPECT(json["rows"][0]["name"].get<std::string>() == "A",
           "Plate JSON 'rows[0]['name']' mismatch: Expected 'A' got '",
           json["rows"][0]["name"].get<std::string>(),
           "'");

    EXPECT(json["rows"][1].contains("name"),
           "Plate JSON 'rows[1]' missing 'name' key");
    EXPECT(json["rows"][1]["name"].get<std::string>() == "B",
           "Plate JSON 'rows[1]['name']' mismatch: Expected 'B' got '",
           json["rows"][1]["name"].get<std::string>(),
           "'");

    EXPECT(json.contains("columns"), "Plate JSON missing 'columns' key");
    EXPECT(json["columns"].is_array(),
           "Plate JSON 'columns' key is not an array");
    EXPECT(json["columns"].size() == 3,
           "Plate JSON 'columns' array size mismatch: Expected 3 got ",
           json["columns"].size());

    EXPECT(json["columns"][0].contains("name"),
           "Plate JSON 'columns[0]' missing 'name' key");
    EXPECT(json["columns"][0]["name"].get<std::string>() == "1",
           "Plate JSON 'columns[0]['name']' mismatch: Expected '1' got '",
           json["columns"][0]["name"].get<std::string>(),
           "'");

    EXPECT(json["columns"][1].contains("name"),
           "Plate JSON 'columns[1]' missing 'name' key");
    EXPECT(json["columns"][1]["name"].get<std::string>() == "2",
           "Plate JSON 'columns[1]['name']' mismatch: Expected '2' got '",
           json["columns"][1]["name"].get<std::string>(),
           "'");

    EXPECT(json["columns"][2].contains("name"),
           "Plate JSON 'columns[2]' missing 'name' key");
    EXPECT(json["columns"][2]["name"].get<std::string>() == "3",
           "Plate JSON 'columns[2]['name']' mismatch: Expected '3' got '",
           json["columns"][2]["name"].get<std::string>(),
           "'");

    const auto& wells_json = json["wells"];
    EXPECT(wells_json.is_array(), "Plate JSON 'wells' key is not an array");
    EXPECT(wells_json.size() == wells.size(),
           "Plate JSON 'wells' array size mismatch: Expected " +
             std::to_string(wells.size()) + " got " +
             std::to_string(wells_json.size()));
    for (auto i = 0; i < wells_json.size(); ++i) {
        const auto& well_json = wells_json[i];
        const auto& well = wells[i];

        EXPECT(well_json.contains("path"),
               "Plate JSON well missing 'path' key");
        EXPECT(well_json["path"] == (well.row_name + "/" + well.column_name),
               "Plate JSON well 'path' key mismatch: Expected ",
               (well.row_name + "/" + well.column_name),
               " got ",
               well_json["path"].get<std::string>());

        EXPECT(well_json.contains("rowIndex"),
               "Plate JSON well missing 'rowIndex' key");
        EXPECT(well_json["rowIndex"] ==
                 static_cast<uint32_t>(i / columns.size()),
               "Plate JSON well 'rowIndex' key mismatch: Expected ",
               static_cast<uint32_t>(i / columns.size()),
               " got ",
               well_json["rowIndex"].get<uint32_t>());

        EXPECT(well_json.contains("columnIndex"),
               "Plate JSON well missing 'columnIndex' key");
        EXPECT(well_json["columnIndex"] ==
                 static_cast<uint32_t>(i % columns.size()),
               "Plate JSON well 'columnIndex' key mismatch: Expected ",
               static_cast<uint32_t>(i % columns.size()),
               " got ",
               well_json["columnIndex"].get<uint32_t>());

        EXPECT(json.contains("acquisitions"),
               "Plate JSON missing 'acquisitions' key");
        const auto& acqs_json = json["acquisitions"];
        EXPECT(acqs_json.is_array(),
               "Plate JSON 'acquisitions' key is not an array");
        EXPECT(acqs_json.size() == acquisitions.size(),
               "Plate JSON 'acquisitions' array size mismatch: Expected ",
               acquisitions.size(),
               " got ",
               acqs_json.size());

        for (auto j = 0; j < acqs_json.size(); ++j) {
            const auto& acq_json = acqs_json[j];
            const auto& acq = acquisitions[j];

            EXPECT(acq_json.contains("id"),
                   "Plate JSON acquisition missing 'id' key");
            EXPECT(acq_json["id"] == acq.id,
                   "Plate JSON acquisition 'id' key mismatch: Expected ",
                   acq.id,
                   " got ",
                   acq_json["id"].get<uint32_t>());
            EXPECT(acq_json.contains("name"),
                   "Plate JSON acquisition missing 'name' key");
            EXPECT(acq_json["name"] == acq.name.value(),
                   "Plate JSON acquisition 'name' key mismatch: Expected ",
                   acq.name.value(),
                   " got ",
                   acq_json["name"].get<std::string>());

            EXPECT(acq_json.contains("maximumfieldcount"),
                   "Plate JSON acquisition missing 'maximumfieldcount' key");
            EXPECT(acq_json["maximumfieldcount"] ==
                     plate.maximum_field_count(acq.id),
                   "Plate JSON acquisition 'maximumfieldcount' key mismatch: "
                   "Expected ",
                   plate.maximum_field_count(acq.id),
                   " got ",
                   acq_json["maximumfieldcount"].get<uint32_t>());

            EXPECT(!acq_json.contains("description"),
                   "Plate JSON acquisition should not have 'description' key");
            EXPECT(!acq_json.contains("starttime"),
                   "Plate JSON acquisition should not have 'starttime' key");
            EXPECT(!acq_json.contains("endtime"),
                   "Plate JSON acquisition should not have 'endtime' key");
        }
    }
}

void
check_sparse_plate()
{
    std::string path = "/data/plate";
    std::string name = "sparse test";
    std::vector<zarr::Well> wells;
    wells.emplace_back(zarr::Well{
      .row_name = "C",
      .column_name = "5",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" } } });
    wells.emplace_back(zarr::Well{
      .row_name = "D",
      .column_name = "7",
      .images = { zarr::FieldOfView{ .acquisition_id = 1, .path = "fov1" } } });

    std::vector<zarr::Acquisition> acquisitions{ zarr::Acquisition{
      .id = 1, .name = "single acquisition", .start_time = 1343731272000ULL } };

    std::vector<std::string> row_names;
    for (auto i = 0; i < 8; ++i) {
        row_names.push_back(std::string(1, 'A' + i));
    }

    std::vector<std::string> column_names;
    for (auto i = 1; i <= 12; ++i) {
        column_names.push_back(std::to_string(i));
    }

    zarr::Plate plate(path, name, row_names, column_names, wells, acquisitions);

    EXPECT(plate.path() == path,
           "Plate path mismatch: Expected ",
           path,
           " got ",
           plate.path());

    EXPECT(plate.name() == name,
           "Plate name mismatch: Expected ",
           name,
           " got ",
           plate.name());

    const auto& test_rows = plate.row_names();
    EXPECT(test_rows.size() == row_names.size(),
           "Expected ",
           row_names.size(),
           " rows, got ",
           test_rows.size());
    for (auto i = 0; i < row_names.size(); ++i) {
        EXPECT(test_rows[i] == row_names[i],
               "Expected row ",
               i,
               " to be '",
               row_names[i],
               "', got '",
               test_rows[i],
               "'");
    }

    const auto& test_columns = plate.column_names();
    EXPECT(test_columns.size() == column_names.size(),
           "Expected ",
           column_names.size(),
           " columns, got ",
           test_columns.size());
    for (auto i = 0; i < column_names.size(); ++i) {
        EXPECT(test_columns[i] == column_names[i],
               "Expected column ",
               i,
               " to be '",
               column_names[i],
               "', got '",
               test_columns[i],
               "'");
    }

    EXPECT(plate.field_count() == 1,
           "Expected field count to be 1, got ",
           plate.field_count());

    EXPECT(plate.maximum_field_count(1) == 1,
           "Expected max field count for Acquisition 1 to be 1, got ",
           plate.maximum_field_count(1));

    // there should be one acquisition
    EXPECT(plate.acquisitions().has_value(), "Expected some acquisitions");
    std::vector<zarr::Acquisition> test_acquisitions = *plate.acquisitions();
    EXPECT(test_acquisitions.size() == 1,
           "Expected 1 acquisition, got ",
           test_acquisitions.size());
    EXPECT(test_acquisitions[0].id == 1,
           "Expected acquisition 0 id to be 1, got ",
           test_acquisitions[0].id);

    EXPECT(test_acquisitions[0].name.has_value(),
           "Expected acquisition 0 to have a name");
    EXPECT(test_acquisitions[0].name.value() == "single acquisition",
           "Expected acquisition 0 name to be 'single acquisition', got '",
           test_acquisitions[0].name.value(),
           "'");
    EXPECT(!test_acquisitions[0].description.has_value(),
           "Expected acquisition 0 to not have a description");
    EXPECT(test_acquisitions[0].start_time.has_value(),
           "Expected acquisition 0 to have a start time");
    EXPECT(test_acquisitions[0].start_time.value() == 1343731272000ULL,
           "Expected acquisition 0 start time to be 1343731272000, got ",
           test_acquisitions[0].start_time.value());
    EXPECT(!test_acquisitions[0].end_time.has_value(),
           "Expected acquisition 0 to not have an end time");
}

int
main()
{
    int retval = 1;

    try {
        check_dense_plate();
        check_sparse_plate();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception during Plate test: " + std::string(e.what()));
    }

    return retval;
}