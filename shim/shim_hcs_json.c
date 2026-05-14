#include "shim_hcs_json.h"

#include "shim_util.h"

#include "zarr/json_writer.h"

#include <stdlib.h>
#include <string.h>

static int
find_row_index(const ZarrHCSPlate* plate, const char* name)
{
    for (size_t i = 0; i < plate->row_count; ++i) {
        if (plate->row_names[i] && strcmp(plate->row_names[i], name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int
find_col_index(const ZarrHCSPlate* plate, const char* name)
{
    for (size_t i = 0; i < plate->column_count; ++i) {
        if (plate->column_names[i] &&
            strcmp(plate->column_names[i], name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

int
shim_hcs_plate_attributes_json(struct strbuf* sb, const ZarrHCSPlate* plate)
{
    struct json_writer jw;
    jw_init(&jw, sb);

    jw_object_begin(&jw); // attributes root

    jw_key(&jw, "ome");
    jw_object_begin(&jw);
    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    jw_key(&jw, "plate");
    jw_object_begin(&jw);
    jw_key(&jw, "name");
    jw_string(&jw, plate->name ? plate->name : "plate");

    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    // field_count = max FOV count across all wells
    int field_count = 0;
    for (size_t w = 0; w < plate->well_count; ++w) {
        int n = (int)plate->wells[w].image_count;
        if (n > field_count) {
            field_count = n;
        }
    }
    jw_key(&jw, "field_count");
    jw_int(&jw, field_count);

    // acquisitions
    jw_key(&jw, "acquisitions");
    jw_array_begin(&jw);
    if (plate->acquisition_count > 0) {
        for (size_t a = 0; a < plate->acquisition_count; ++a) {
            const ZarrHCSAcquisition* acq = &plate->acquisitions[a];

            // Compute maximumfieldcount for this acquisition:
            // count how many FOVs reference this acquisition across all wells
            int max_fov_count = 0;
            for (size_t w = 0; w < plate->well_count; ++w) {
                const ZarrHCSWell* well = &plate->wells[w];
                int count = 0;
                for (size_t f = 0; f < well->image_count; ++f) {
                    if (well->images[f].has_acquisition_id &&
                        well->images[f].acquisition_id == acq->id) {
                        ++count;
                    }
                }
                if (count > max_fov_count) {
                    max_fov_count = count;
                }
            }

            jw_object_begin(&jw);
            jw_key(&jw, "id");
            jw_int(&jw, (int64_t)acq->id);
            jw_key(&jw, "maximumfieldcount");
            jw_int(&jw, max_fov_count);
            if (acq->name) {
                jw_key(&jw, "name");
                jw_string(&jw, acq->name);
            }
            if (acq->has_start_time) {
                jw_key(&jw, "starttime");
                jw_uint(&jw, acq->start_time);
            }
            if (acq->has_end_time) {
                jw_key(&jw, "endtime");
                jw_uint(&jw, acq->end_time);
            }
            jw_object_end(&jw);
        }
    } else {
        // Single default acquisition
        jw_object_begin(&jw);
        jw_key(&jw, "id");
        jw_int(&jw, 0);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    // columns
    jw_key(&jw, "columns");
    jw_array_begin(&jw);
    for (size_t c = 0; c < plate->column_count; ++c) {
        jw_object_begin(&jw);
        jw_key(&jw, "name");
        jw_string(&jw, plate->column_names[c]);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    // rows
    jw_key(&jw, "rows");
    jw_array_begin(&jw);
    for (size_t r = 0; r < plate->row_count; ++r) {
        jw_object_begin(&jw);
        jw_key(&jw, "name");
        jw_string(&jw, plate->row_names[r]);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    // wells
    jw_key(&jw, "wells");
    jw_array_begin(&jw);
    for (size_t w = 0; w < plate->well_count; ++w) {
        const ZarrHCSWell* well = &plate->wells[w];
        int row_idx = find_row_index(plate, well->row_name);
        int col_idx = find_col_index(plate, well->column_name);

        jw_object_begin(&jw);
        jw_key(&jw, "path");
        char* path =
          shim_alloc_printf("%s/%s", well->row_name, well->column_name);
        if (!path) {
            return 1;
        }
        jw_string(&jw, path);
        free(path);
        jw_key(&jw, "rowIndex");
        jw_int(&jw, row_idx);
        jw_key(&jw, "columnIndex");
        jw_int(&jw, col_idx);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    jw_object_end(&jw); // plate
    jw_object_end(&jw); // ome
    jw_object_end(&jw); // attributes root

    return jw_error(&jw) ? 1 : 0;
}

int
shim_hcs_well_attributes_json(struct strbuf* sb, const ZarrHCSWell* well)
{
    struct json_writer jw;
    jw_init(&jw, sb);

    jw_object_begin(&jw); // attributes root

    jw_key(&jw, "ome");
    jw_object_begin(&jw);
    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    jw_key(&jw, "well");
    jw_object_begin(&jw);

    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    jw_key(&jw, "images");
    jw_array_begin(&jw);
    for (size_t f = 0; f < well->image_count; ++f) {
        const ZarrHCSFieldOfView* fov = &well->images[f];
        jw_object_begin(&jw);
        jw_key(&jw, "acquisition");
        if (fov->has_acquisition_id) {
            jw_int(&jw, (int64_t)fov->acquisition_id);
        } else {
            jw_int(&jw, 0);
        }
        jw_key(&jw, "path");
        jw_string(&jw, fov->path ? fov->path : "0");
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    jw_object_end(&jw); // well
    jw_object_end(&jw); // ome
    jw_object_end(&jw); // attributes root

    return jw_error(&jw) ? 1 : 0;
}
