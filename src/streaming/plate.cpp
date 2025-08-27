#include "macros.hh"
#include "plate.hh"

#include <algorithm>
#include <regex>
#include <unordered_set>

nlohmann::json
zarr::Acquisition::to_json() const
{
    // Each acquisition object MUST contain an id key whose value MUST be an
    // unique integer identifier greater than or equal to 0 within the context
    // of the plate to which fields of view can refer to (see #well-md). Each
    // acquisition object SHOULD contain a name key whose value MUST be a string
    // identifying the name of the acquisition. Each acquisition object SHOULD
    // contain a maximumfieldcount key whose value MUST be a positive integer
    // indicating the maximum number of fields of view for the acquisition. Each
    // acquisition object MAY contain a description key whose value MUST be a
    // string specifying a description for the acquisition. Each acquisition
    // object MAY contain a starttime and/or endtime key whose values MUST be
    // integer epoch timestamps specifying the start and/or end timestamp of the
    // acquisition.
    nlohmann::json j;
    j["id"] = id;
    if (name.has_value()) {
        j["name"] = name.value();
    }
    if (description.has_value()) {
        j["description"] = description.value();
    }
    if (start_time.has_value()) {
        j["starttime"] = start_time.value();
    }
    if (end_time.has_value()) {
        j["endtime"] = end_time.value();
    }

    return j;
}

zarr::Plate::Plate(const std::string& path,
                   const std::string& name,
                   const std::vector<std::string>& row_names,
                   const std::vector<std::string>& column_names,
                   const std::vector<Well>& wells,
                   const std::optional<std::vector<Acquisition>>& acquisitions)
  : path_(path)
  , name_(name)
  , row_names_(row_names)
  , column_names_(column_names)
  , wells_(wells)
  , acquisitions_(acquisitions)
  , field_count_(0)
{
    compute_field_counts_();
    validate_acquisitions_();
    validate_wells_();
}

const std::string&
zarr::Plate::path() const
{
    return path_;
}

const std::string&
zarr::Plate::name() const
{
    return name_;
}

const std::vector<zarr::Well>&
zarr::Plate::wells() const
{
    return wells_;
}

const std::optional<std::vector<zarr::Acquisition>>&
zarr::Plate::acquisitions() const
{
    return acquisitions_;
}

uint32_t
zarr::Plate::field_count() const
{
    return field_count_;
}

uint32_t
zarr::Plate::maximum_field_count(uint32_t acquisition) const
{
    auto it = max_field_counts_.find(acquisition);
    EXPECT(it != max_field_counts_.end(),
           "Acquisition ID not found in plate: " + std::to_string(acquisition));
    return it->second;
}

const std::vector<std::string>&
zarr::Plate::column_names() const
{
    return column_names_;
}

const std::vector<std::string>&
zarr::Plate::row_names() const
{
    return row_names_;
}

void
zarr::Plate::compute_field_counts_()
{
    // Each acquisition object SHOULD contain a `maximumfieldcount` key whose
    // value MUST be a positive integer indicating the maximum number of fields
    // of view for the acquisition.
    for (const auto& well : wells_) {
        std::unordered_map<uint32_t, uint32_t> well_field_counts;

        for (const auto& image : well.images) {
            if (well_field_counts.find(image.acquisition_id) ==
                well_field_counts.end()) {
                well_field_counts[image.acquisition_id] = 0;
            }

            ++well_field_counts[image.acquisition_id];
        }

        for (const auto& [acq_id, count] : well_field_counts) {
            if (max_field_counts_.find(acq_id) == max_field_counts_.end()) {
                max_field_counts_[acq_id] = 0;
            }

            max_field_counts_[acq_id] =
              std::max(max_field_counts_[acq_id], count);
        }
    }

    // The plate dictionary SHOULD contain a field_count key whose value MUST be
    // a positive integer defining the maximum number of fields per view across
    // all wells.
    field_count_ = 0;
    for (const auto& well : wells_) {
        uint32_t well_count = 0;
        for (const auto& image : well.images) {
            ++well_count;
        }
        field_count_ = std::max(field_count_, well_count);
    }
}

void
zarr::Plate::validate_acquisitions_()
{
    if (!acquisitions_.has_value()) {
        EXPECT(max_field_counts_.empty(),
               "Wells contain acquisitions but no acquisitions were given");

        return;
    }

    // Each acquisition object MUST contain an id key whose value MUST be an
    // unique integer identifier greater than or equal to 0 within the context
    // of the plate to which fields of view can refer to.
    std::unordered_set<uint32_t> unique_ids;
    for (const auto& acq : *acquisitions_) {
        EXPECT(unique_ids.find(acq.id) == unique_ids.end(),
               "Duplicate acquisition ID: ",
               acq.id);
        unique_ids.insert(acq.id);
    }

    for (const auto& [acq_id, count] : max_field_counts_) {
        EXPECT(unique_ids.find(acq_id) != unique_ids.end(),
               "Acquisition ID ",
               acq_id,
               " found in wells but not in acquisitions");
    }
}

void
zarr::Plate::validate_wells_()
{
    std::regex valid_name_regex("^[A-Za-z0-9]+$");

    // Each defined row MUST contain a name key whose value MUST be a string
    // defining the row name. The name MUST contain only alphanumeric
    // characters, MUST be case-sensitive, and MUST NOT be a duplicate of any
    // other name in the rows list. Care SHOULD be taken to avoid collisions on
    // case-insensitive filesystems (e.g. avoid using both Aa and aA).
    std::unordered_set<std::string> unique_rows(row_names_.begin(),
                                                row_names_.end());
    EXPECT(unique_rows.size() == row_names_.size(),
           "Row names contain duplicates");

    std::unordered_set<std::string> lowercase_row_names;
    for (const auto& row : unique_rows) {
        EXPECT(!row.empty(), "Row name is empty");
        EXPECT(std::regex_match(row, valid_name_regex),
               "Row name '" + row + "' contains invalid characters");

        std::string lower_row = row;
        std::transform(lower_row.begin(),
                       lower_row.end(),
                       lower_row.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        lowercase_row_names.insert(lower_row);
    }

    EXPECT(lowercase_row_names.size() == unique_rows.size(),
           "Row names contain case-insensitive duplicates");

    // The name MUST contain only alphanumeric characters, MUST be
    // case-sensitive, and MUST NOT be a duplicate of any other name in the
    // columns list. Care SHOULD be taken to avoid collisions on
    // case-insensitive filesystems (e.g. avoid using both Aa and aA).
    std::unordered_set<std::string> unique_columns(column_names_.begin(),
                                                   column_names_.end());
    EXPECT(unique_columns.size() == column_names_.size(),
           "Column names contain duplicates");

    std::unordered_set<std::string> lowercase_column_names;
    for (const auto& column : unique_columns) {
        EXPECT(!column.empty(), "Column name is empty");
        EXPECT(std::regex_match(column, valid_name_regex),
               "Column name '" + column + "' contains invalid characters");

        std::string lower_column = column;
        std::transform(lower_column.begin(),
                       lower_column.end(),
                       lower_column.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        lowercase_column_names.insert(lower_column);
    }

    EXPECT(lowercase_column_names.size() == unique_columns.size(),
           "Column names contain case-insensitive duplicates");

    // make sure all the well row/col names are in the unique lists
    for (const auto& well : wells_) {
        EXPECT(!well.row_name.empty(), "Well row name is empty");
        EXPECT(unique_rows.find(well.row_name) != unique_rows.end(),
               "Well row name '",
               well.row_name,
               "' not found in given row names");

        EXPECT(!well.column_name.empty(), "Well column name is empty");
        EXPECT(unique_columns.find(well.column_name) != unique_columns.end(),
               "Well column name '",
               well.column_name,
               "' not found in given column names");
    }
}

nlohmann::json
zarr::Plate::to_json() const
{
    // See https://ngff.openmicroscopy.org/latest/#plate-md
    nlohmann::json j;

    // The plate dictionary MUST contain a version key whose value MUST be a
    // string specifying the version of the plate specification.
    j["version"] = "0.5";

    // The plate dictionary SHOULD contain a name key whose value MUST be a
    // string defining the name of the plate.
    j["name"] = name_;

    // The plate dictionary SHOULD contain a field_count key whose value MUST be
    // a positive integer defining the maximum number of fields per view across
    // all wells.
    j["field_count"] = field_count_;

    // The plate dictionary MUST contain a rows key whose value MUST be a list
    // of JSON objects defining the rows of the plate. Each row object defines
    // the properties of the row at the index of the object in the list. Each
    // row in the physical plate MUST be defined, even if no wells in the row
    // are defined. Each defined row MUST contain a name key whose value MUST be
    // a string defining the row name. The name MUST contain only alphanumeric
    // characters, MUST be case-sensitive, and MUST NOT be a duplicate of any
    // other name in the rows list. Care SHOULD be taken to avoid collisions on
    // case-insensitive filesystems (e.g. avoid using both Aa and aA).
    j["rows"] = nlohmann::json::array();
    for (const auto& row : row_names_) {
        j["rows"].push_back({ { "name", row } });
    }

    // The plate dictionary MUST contain a columns key whose value MUST be a
    // list of JSON objects defining the columns of the plate. Each column
    // object defines the properties of the column at the index of the object in
    // the list. Each column in the physical plate MUST be defined, even if no
    // wells in the column are defined. Each column object MUST contain a name
    // key whose value is a string specifying the column name. The name MUST
    // contain only alphanumeric characters, MUST be case-sensitive, and MUST
    // NOT be a duplicate of any other name in the columns list. Care SHOULD be
    // taken to avoid collisions on case-insensitive filesystems (e.g. avoid
    // using both Aa and aA).
    j["columns"] = nlohmann::json::array();
    for (const auto& column : column_names_) {
        j["columns"].push_back({ { "name", column } });
    }

    // The plate dictionary MUST contain a wells key whose value MUST be a list
    // of JSON objects defining the wells of the plate. Each well object MUST
    // contain a path key whose value MUST be a string specifying the path to
    // the well subgroup. The path MUST consist of a name in the rows list, a
    // file separator (/), and a name from the columns list, in that order. The
    // path MUST NOT contain additional leading or trailing directories. Each
    // well object MUST contain both a rowIndex key whose value MUST be an
    // integer identifying the index into the rows list and a columnIndex key
    // whose value MUST be an integer identifying the index into the columns
    // list. rowIndex and columnIndex MUST be 0-based. The rowIndex,
    // columnIndex, and path MUST all refer to the same row/column pair.
    j["wells"] = nlohmann::json::array();
    for (const auto& well : wells_) {
        nlohmann::json w;
        w["path"] = well.row_name + "/" + well.column_name;
        w["rowIndex"] = static_cast<uint32_t>(std::distance(
          row_names_.begin(),
          std::find(row_names_.begin(), row_names_.end(), well.row_name)));
        w["columnIndex"] = static_cast<uint32_t>(std::distance(
          column_names_.begin(),
          std::find(
            column_names_.begin(), column_names_.end(), well.column_name)));

        j["wells"].push_back(w);
    }

    // The plate dictionary MAY contain an acquisitions key whose value MUST be
    // a list of JSON objects defining the acquisitions for a given plate to
    // which wells can refer to.
    if (acquisitions_.has_value()) {
        j["acquisitions"] = nlohmann::json::array();
        for (const auto& acq : *acquisitions_) {
            j["acquisitions"].push_back(acq.to_json());
            j["acquisitions"].back()["maximumfieldcount"] =
              maximum_field_count(acq.id); // MUST be added here
        }
    }

    return j;
}