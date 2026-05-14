#include "acquire.zarr.h"
#include "shim_backend.h"
#include "shim_convert.h"
#include "shim_util.h"

#include "dtype.h"

#include <stdlib.h>
#include <string.h>

/* --- Allocator helpers -------------------------------------------------- */

ZarrStatusCode
ZarrStreamSettings_create_arrays(ZarrStreamSettings* settings,
                                 size_t array_count)
{
    if (!settings) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrArraySettings* arrays = calloc(array_count, sizeof(ZarrArraySettings));
    if (!arrays) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrStreamSettings_destroy_arrays(settings);
    settings->arrays = arrays;
    settings->array_count = array_count;

    return ZarrStatusCode_Success;
}

void
ZarrStreamSettings_destroy_arrays(ZarrStreamSettings* settings)
{
    if (!settings) {
        return;
    }
    if (!settings->arrays) {
        settings->array_count = 0;
        return;
    }
    for (size_t i = 0; i < settings->array_count; ++i) {
        ZarrArraySettings_destroy_dimension_array(&settings->arrays[i]);
    }
    free(settings->arrays);
    settings->arrays = NULL;
    settings->array_count = 0;
}

ZarrStatusCode
ZarrArraySettings_create_dimension_array(ZarrArraySettings* settings,
                                         size_t dimension_count)
{
    if (!settings) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (dimension_count < 2) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrDimensionProperties* dims =
      calloc(dimension_count, sizeof(ZarrDimensionProperties));
    if (!dims) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrArraySettings_destroy_dimension_array(settings);
    settings->dimensions = dims;
    settings->dimension_count = dimension_count;

    return ZarrStatusCode_Success;
}

void
ZarrArraySettings_destroy_dimension_array(ZarrArraySettings* settings)
{
    if (!settings) {
        return;
    }
    free(settings->dimensions);
    settings->dimensions = NULL;
    settings->dimension_count = 0;
}

ZarrStatusCode
ZarrHCSWell_create_image_array(ZarrHCSWell* well, size_t image_count)
{
    if (!well) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (image_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSFieldOfView* images =
      calloc(image_count, sizeof(ZarrHCSFieldOfView));
    if (!images) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSWell_destroy_image_array(well);
    well->images = images;
    well->image_count = image_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSWell_destroy_image_array(ZarrHCSWell* well)
{
    if (!well) {
        return;
    }
    if (well->images) {
        for (size_t i = 0; i < well->image_count; ++i) {
            if (well->images[i].array_settings) {
                ZarrArraySettings_destroy_dimension_array(
                  well->images[i].array_settings);
                well->images[i].array_settings = NULL;
            }
        }
        free(well->images);
        well->images = NULL;
    }
    well->image_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_well_array(ZarrHCSPlate* plate, size_t well_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (well_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSWell* wells = calloc(well_count, sizeof(ZarrHCSWell));
    if (!wells) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_well_array(plate);
    plate->wells = wells;
    plate->well_count = well_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_well_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    if (plate->wells) {
        for (size_t i = 0; i < plate->well_count; ++i) {
            ZarrHCSWell_destroy_image_array(&plate->wells[i]);
        }
        free(plate->wells);
        plate->wells = NULL;
    }
    plate->well_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_acquisition_array(ZarrHCSPlate* plate,
                                      size_t acquisition_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (acquisition_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSAcquisition* acqs =
      calloc(acquisition_count, sizeof(ZarrHCSAcquisition));
    if (!acqs) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_acquisition_array(plate);
    plate->acquisitions = acqs;
    plate->acquisition_count = acquisition_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_acquisition_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    free(plate->acquisitions);
    plate->acquisitions = NULL;
    plate->acquisition_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_row_name_array(ZarrHCSPlate* plate, size_t row_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (row_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    const char** names = calloc(row_count, sizeof(const char*));
    if (!names) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_row_name_array(plate);
    plate->row_names = names;
    plate->row_count = row_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_row_name_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    free((void*)plate->row_names);
    plate->row_names = NULL;
    plate->row_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_column_name_array(ZarrHCSPlate* plate, size_t column_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (column_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    const char** names = calloc(column_count, sizeof(const char*));
    if (!names) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_column_name_array(plate);
    plate->column_names = names;
    plate->column_count = column_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_column_name_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    free((void*)plate->column_names);
    plate->column_names = NULL;
    plate->column_count = 0;
}

ZarrStatusCode
ZarrHCSSettings_create_plate_array(ZarrHCSSettings* settings,
                                   size_t plate_count)
{
    if (!settings) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (plate_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSPlate* plates = calloc(plate_count, sizeof(ZarrHCSPlate));
    if (!plates) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSSettings_destroy_plate_array(settings);
    settings->plates = plates;
    settings->plate_count = plate_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSSettings_destroy_plate_array(ZarrHCSSettings* settings)
{
    if (!settings) {
        return;
    }
    if (settings->plates) {
        for (size_t i = 0; i < settings->plate_count; ++i) {
            ZarrHCSPlate_destroy_well_array(&settings->plates[i]);
            ZarrHCSPlate_destroy_acquisition_array(&settings->plates[i]);
            ZarrHCSPlate_destroy_row_name_array(&settings->plates[i]);
            ZarrHCSPlate_destroy_column_name_array(&settings->plates[i]);
        }
        free(settings->plates);
        settings->plates = NULL;
    }
    settings->plate_count = 0;
}

/* --- Settings queries --------------------------------------------------- */

// Estimate the heap+frame bytes a single array will use. HCS FOVs are always
// multiscale; for flat arrays pass as->multiscale. Returns 0 on success.
static int
estimate_one_array_bytes(const ZarrArraySettings* as,
                         bool force_multiscale,
                         size_t* out_bytes)
{
    const size_t ndims = as->dimension_count;
    if (ndims < 2 || !as->dimensions) {
        return 1;
    }

    enum dtype dt = shim_convert_dtype(as->data_type);
    struct codec_config codec = shim_convert_codec(as->compression_settings);
    struct dimension* dims =
      shim_convert_dimensions(as->dimensions,
                              ndims,
                              as->storage_dimension_order,
                              force_multiscale || as->multiscale);
    if (!dims) {
        return 1;
    }

    size_t frame_bytes = dtype_bpe(dt) *
                         as->dimensions[ndims - 2].array_size_px *
                         as->dimensions[ndims - 1].array_size_px;

    struct tile_stream_configuration cfg = {
        .buffer_capacity_bytes = frame_bytes,
        .dtype = dt,
        .rank = (uint8_t)ndims,
        .dimensions = dims,
        .codec = codec,
        .reduce_method = shim_convert_reduce_method(as->downsampling_method),
        .append_reduce_method =
          shim_convert_reduce_method(as->downsampling_method),
    };

    tile_stream_memory_info_t info = { 0 };
    int err = tile_stream_memory_estimate(&cfg, 0, &info);
    free(dims);

    if (err) {
        return 1;
    }

    *out_bytes = TILE_STREAM_TOTAL_BYTES(info) + frame_bytes;
    return 0;
}

ZarrStatusCode
ZarrStreamSettings_estimate_max_memory_usage(const ZarrStreamSettings* settings,
                                             size_t* usage)
{
    if (!settings || !usage) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (!settings->arrays && !settings->hcs_settings) {
        return ZarrStatusCode_InvalidArgument;
    }

    size_t total = 0;

    for (size_t i = 0; i < settings->array_count; ++i) {
        size_t bytes = 0;
        if (estimate_one_array_bytes(&settings->arrays[i], false, &bytes)) {
            return ZarrStatusCode_InternalError;
        }
        total += bytes;
    }

    if (settings->hcs_settings) {
        const ZarrHCSSettings* hcs = settings->hcs_settings;
        for (size_t p = 0; p < hcs->plate_count; ++p) {
            const ZarrHCSPlate* plate = &hcs->plates[p];
            for (size_t w = 0; w < plate->well_count; ++w) {
                const ZarrHCSWell* well = &plate->wells[w];
                for (size_t f = 0; f < well->image_count; ++f) {
                    const ZarrArraySettings* as =
                      well->images[f].array_settings;
                    if (!as) {
                        return ZarrStatusCode_InvalidArgument;
                    }
                    size_t bytes = 0;
                    if (estimate_one_array_bytes(as, true, &bytes)) {
                        return ZarrStatusCode_InternalError;
                    }
                    total += bytes;
                }
            }
        }
    }

    *usage = total;
    return ZarrStatusCode_Success;
}

size_t
ZarrStreamSettings_get_array_count(const ZarrStreamSettings* settings)
{
    if (!settings) {
        return 0;
    }

    size_t count = settings->array_count;

    if (settings->hcs_settings) {
        const ZarrHCSSettings* hcs = settings->hcs_settings;
        for (size_t i = 0; i < hcs->plate_count; ++i) {
            const ZarrHCSPlate* plate = &hcs->plates[i];
            for (size_t j = 0; j < plate->well_count; ++j) {
                count += plate->wells[j].image_count;
            }
        }
    }

    return count;
}

ZarrStatusCode
ZarrStreamSettings_get_array_key(const ZarrStreamSettings* settings,
                                 size_t index,
                                 char** key)
{
    if (!settings || !key) {
        return ZarrStatusCode_InvalidArgument;
    }

    // Flat arrays first
    if (index < settings->array_count) {
        const ZarrArraySettings* as = &settings->arrays[index];
        if (!as->output_key) {
            *key = NULL;
            return ZarrStatusCode_Success;
        }
        *key = strdup(as->output_key);
        return *key ? ZarrStatusCode_Success : ZarrStatusCode_OutOfMemory;
    }

    // HCS FOVs
    size_t idx = settings->array_count;
    if (settings->hcs_settings) {
        const ZarrHCSSettings* hcs = settings->hcs_settings;
        for (size_t p = 0; p < hcs->plate_count; ++p) {
            const ZarrHCSPlate* plate = &hcs->plates[p];
            for (size_t w = 0; w < plate->well_count; ++w) {
                const ZarrHCSWell* well = &plate->wells[w];
                for (size_t f = 0; f < well->image_count; ++f) {
                    if (idx == index) {
                        const ZarrHCSFieldOfView* fov = &well->images[f];
                        const char* plate_path =
                          plate->path ? plate->path : "plate";
                        const char* fov_path = fov->path ? fov->path : "0";

                        char* buf = shim_alloc_printf("%s/%s/%s/%s",
                                                      plate_path,
                                                      well->row_name,
                                                      well->column_name,
                                                      fov_path);
                        if (!buf) {
                            return ZarrStatusCode_OutOfMemory;
                        }
                        *key = buf;
                        return ZarrStatusCode_Success;
                    }
                    ++idx;
                }
            }
        }
    }

    return ZarrStatusCode_InvalidIndex;
}
