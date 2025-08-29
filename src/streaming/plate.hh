#pragma once

#include <nlohmann/json.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace zarr {
struct FieldOfView
{
    std::optional<uint32_t> acquisition_id;
    std::string path; // relative to the well: base_path/row/col/fov_path
};

struct Well
{
    std::string row_name;
    std::string column_name; // metadata: base_path/row_name/col_name/zarr.json
    std::vector<FieldOfView> images;

    nlohmann::json to_json() const;
};

struct Acquisition
{
    uint32_t id; // unique identifier (mandatory)
    std::optional<std::string> name;
    std::optional<std::string> description;
    std::optional<uint64_t> start_time;
    std::optional<uint64_t> end_time;

    nlohmann::json to_json() const;
};

struct Plate
{
    Plate(const std::string& path,
          const std::string& name,
          const std::vector<std::string>& row_names,
          const std::vector<std::string>& column_names,
          const std::vector<Well>& wells,
          const std::optional<std::vector<Acquisition>>& acquisitions =
            std::nullopt);

    const std::string& path() const;
    const std::string& name() const;
    const std::vector<Well>& wells() const;
    const std::optional<std::vector<Acquisition>>& acquisitions() const;

    uint32_t field_count() const;
    uint32_t maximum_field_count(uint32_t acquisition) const;
    const std::vector<std::string>& row_names() const;
    const std::vector<std::string>& column_names() const;

    nlohmann::json to_json() const;

  private:
    std::string path_; // relative to the root: root/path_
    std::string name_;
    std::vector<std::string> row_names_;
    std::vector<std::string> column_names_;
    std::vector<Well> wells_;
    std::optional<std::vector<Acquisition>> acquisitions_;

    uint32_t field_count_;
    std::unordered_map<uint32_t, uint32_t> max_field_counts_;

    void compute_field_counts_();
    void validate_acquisitions_();
    void validate_wells_();
};
} // namespace zarr