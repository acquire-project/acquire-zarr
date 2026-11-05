#pragma once

#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <string>

/// The HCS plate of the HCS integration tests, built as a caller builds it:
/// each field of view is an OME-NGFF array at <plate>/<row>/<column>/<field>,
/// and the plate and well metadata are attributes under `ome`.
namespace hcs {
constexpr size_t field_count = 3;

inline const nlohmann::json plate_ome = {
    { "version", "0.5" },
    { "plate",
      {
        { "name", "Test Plate" },
        { "field_count", 2 },
        { "acquisitions",
          {
            {
              { "id", 0 },
              { "maximumfieldcount", 1 },
              { "name", "Meas_01(2012-07-31_10-41-12)" },
              { "starttime", 1343731272000ULL },
            },
            {
              { "id", 1 },
              { "maximumfieldcount", 1 },
              { "name", "Meas_02(2012-07-31_10-45-12)" },
              { "starttime", 1343735801000ULL },
              { "endtime", 1343737645000ULL },
            },
          } },
        { "columns",
          { { { "name", "1" } },
            { { "name", "2" } },
            { { "name", "3" } },
            { { "name", "4" } },
            { { "name", "5" } },
            { { "name", "6" } },
            { { "name", "7" } },
            { { "name", "8" } },
            { { "name", "9" } },
            { { "name", "10" } },
            { { "name", "11" } },
            { { "name", "12" } } } },
        { "rows",
          { { { "name", "A" } },
            { { "name", "B" } },
            { { "name", "C" } },
            { { "name", "D" } },
            { { "name", "E" } },
            { { "name", "F" } },
            { { "name", "G" } },
            { { "name", "H" } } } },
        { "wells",
          {
            { { "path", "C/5" }, { "rowIndex", 2 }, { "columnIndex", 4 } },
            { { "path", "D/7" }, { "rowIndex", 3 }, { "columnIndex", 6 } },
          } },
      } },
};

inline const nlohmann::json c5_ome = {
    { "version", "0.5" },
    { "well",
      { { "images",
          {
            { { "acquisition", 0 }, { "path", "fov1" } },
            { { "acquisition", 1 }, { "path", "fov2" } },
          } } } },
};

inline const nlohmann::json d7_ome = {
    { "version", "0.5" },
    { "well",
      { { "images", { { { "acquisition", 0 }, { "path", "fov1" } } } } } },
};

/// Fill @p fields (field_count entries) with the three fields of view.
/// Free them with destroy_fields().
inline void
configure_fields(ZarrArraySettings* fields)
{
    auto set_dims = [](ZarrArraySettings* a,
                       std::initializer_list<ZarrDimensionProperties> dims) {
        CHECK_OK(ZarrArraySettings_create_dimension_array(a, dims.size()));
        size_t i = 0;
        for (const auto& d : dims) {
            a->dimensions[i++] = d;
        }
    };

    fields[0] = {
        .output_key = "test_plate/C/5/fov1",
        .data_type = ZarrDataType_uint8,
        .is_ngff = true,
    };
    set_dims(fields,
             {
               DIM("z", ZarrDimensionType_Space, 0, 1, 1, nullptr, 1.0),
               DIM("y", ZarrDimensionType_Space, 480, 256, 4, nullptr, 1.0),
               DIM("x", ZarrDimensionType_Space, 640, 256, 4, nullptr, 1.0),
             });

    fields[1] = {
        .output_key = "test_plate/C/5/fov2",
        .data_type = ZarrDataType_uint16,
        .is_ngff = true,
    };
    set_dims(fields + 1,
             {
               DIM("z", ZarrDimensionType_Space, 0, 1, 1, nullptr, 1.0),
               DIM("y", ZarrDimensionType_Space, 480, 256, 4, nullptr, 1.0),
               DIM("x", ZarrDimensionType_Space, 640, 256, 4, nullptr, 1.0),
             });

    fields[2] = {
        .output_key = "test_plate/D/7/fov1",
        .data_type = ZarrDataType_uint16,
        .is_ngff = true,
    };
    set_dims(fields + 2,
             {
               DIM("t", ZarrDimensionType_Time, 10, 1, 1, nullptr, 1.0),
               DIM("c", ZarrDimensionType_Channel, 3, 1, 1, nullptr, 1.0),
               DIM("z", ZarrDimensionType_Space, 5, 1, 1, nullptr, 1.0),
               DIM("y", ZarrDimensionType_Space, 512, 256, 4, nullptr, 1.0),
               DIM("x", ZarrDimensionType_Space, 512, 256, 4, nullptr, 1.0),
             });
}

inline void
destroy_fields(ZarrArraySettings* fields)
{
    for (size_t i = 0; i < field_count; ++i) {
        ZarrArraySettings_destroy_dimension_array(fields + i);
    }
}

/// Write the plate and well metadata, as a caller does after create.
inline void
write_metadata(ZarrStream* stream)
{
    for (const auto& [key, ome] : { std::pair{ "test_plate", &plate_ome },
                                    std::pair{ "test_plate/C/5", &c5_ome },
                                    std::pair{ "test_plate/D/7", &d7_ome } }) {
        const std::string json = ome->dump();
        const auto status =
          ZarrStream_write_attributes(stream, key, "ome", json.c_str());
        EXPECT(status == ZarrStatusCode_Success,
               "Failed to write metadata to '",
               key,
               "': ",
               Zarr_get_status_message(status));
    }
}
} // namespace hcs
