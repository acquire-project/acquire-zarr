#include "shim_array.h"

#include "shim_convert.h"
#include "shim_util.h"

#include "log/log.h"
#include "multiarray/multiarray.h"
#include "util/prelude.h"
#include "zarr/store.h"

#include <stdlib.h>
#include <string.h>

// Reset the fields this module owns (everything except sa->key) to a
// zeroed / SINK_NONE state so shim_array_destroy is safe to call whether
// or not the configure helpers ran to completion.
static void
reset_owned_fields(struct shim_array* sa)
{
    free(sa->dims);
    free(sa->axes);
    sa->rank = 0;
    sa->dims = NULL;
    sa->axes = NULL;
    sa->frame_bytes = 0;
    sa->sink = (struct shim_sink){ .kind = SHIM_SINK_NONE };
    memset(&sa->config, 0, sizeof(sa->config));
}

int
shim_configure_multiscale_array(struct ZarrStream_s* stream,
                                const ZarrArraySettings* as,
                                struct shim_array* sa)
{
    // sa->key is caller-owned; everything else is ours. Zero-init at
    // entry and free-and-null on any failure so the struct is equivalent
    // to a fresh calloc() regardless of what the caller passed in.
    reset_owned_fields(sa);

    sa->rank = (uint8_t)as->dimension_count;
    sa->dims = shim_convert_dimensions(
      as->dimensions, as->dimension_count, as->storage_dimension_order, true);
    CHECK(Fail, sa->dims);

    sa->axes = shim_convert_ngff_axes(as->dimensions, as->dimension_count);
    CHECK(Fail, sa->axes);

    enum dtype dt = shim_convert_dtype(as->data_type);
    struct codec_config codec = shim_convert_codec(as->compression_settings);

    size_t ndims = as->dimension_count;
    sa->frame_bytes = dtype_bpe(dt) * as->dimensions[ndims - 2].array_size_px *
                      as->dimensions[ndims - 1].array_size_px;

    struct ngff_multiscale_config ms_cfg = {
        .data_type = dt,
        .fill_value = 0.0,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .nlod = 0,
        .codec = codec,
        .axes = sa->axes,
    };
    struct ngff_multiscale* ms =
      ngff_multiscale_create(stream->store, sa->key, &ms_cfg);
    CHECK(Fail, ms);
    sa->sink.kind = SHIM_SINK_MULTISCALE;
    sa->sink.multiscale = ms;

    sa->config = (struct tile_stream_configuration){
        .buffer_capacity_bytes = sa->frame_bytes,
        .dtype = dt,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .codec = codec,
        .reduce_method = shim_convert_reduce_method(as->downsampling_method),
        .append_reduce_method =
          shim_convert_reduce_method(as->downsampling_method),
        .epochs_per_batch = 0,
        .target_batch_bytes = 0,
        .metadata_update_interval_s = 1.0f,
        .max_threads = stream->max_threads,
    };

    return 1;

Fail:
    reset_owned_fields(sa);
    return 0;
}

int
shim_create_flat_array(struct ZarrStream_s* stream,
                       const ZarrArraySettings* as,
                       struct shim_array* sa)
{
    reset_owned_fields(sa);

    if (as->output_key) {
        free(sa->key);
        sa->key = strdup(as->output_key);
        CHECK(Fail, sa->key);
    }

    if (as->multiscale) {
        // Delegate; multiscale helper resets owned fields on failure.
        return shim_configure_multiscale_array(stream, as, sa);
    }

    sa->rank = (uint8_t)as->dimension_count;
    sa->dims = shim_convert_dimensions(
      as->dimensions, as->dimension_count, as->storage_dimension_order, false);
    CHECK(Fail, sa->dims);

    enum dtype dt = shim_convert_dtype(as->data_type);
    struct codec_config codec = shim_convert_codec(as->compression_settings);

    size_t ndims = as->dimension_count;
    sa->frame_bytes = dtype_bpe(dt) * as->dimensions[ndims - 2].array_size_px *
                      as->dimensions[ndims - 1].array_size_px;

    struct zarr_array_config arr_cfg = {
        .data_type = dt,
        .fill_value = 0.0,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .codec = codec,
    };

    // Write intermediate group zarr.json for each path component and ensure
    // the leaf directory exists for zarr_array_create.
    CHECK(Fail, shim_write_intermediate_groups(stream->store, sa->key) == 0);
    if (sa->key) {
        CHECK(Fail, stream->store->mkdirs(stream->store, sa->key) == 0);
    }

    struct zarr_array* arr =
      zarr_array_create(stream->store, sa->key, &arr_cfg);
    CHECK(Fail, arr);
    sa->sink.kind = SHIM_SINK_ARRAY;
    sa->sink.array = arr;

    sa->config = (struct tile_stream_configuration){
        .buffer_capacity_bytes = sa->frame_bytes,
        .dtype = dt,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .codec = codec,
        .reduce_method = shim_convert_reduce_method(as->downsampling_method),
        .append_reduce_method =
          shim_convert_reduce_method(as->downsampling_method),
        .epochs_per_batch = 0,
        .target_batch_bytes = 0,
        .metadata_update_interval_s = 1.0f,
        .max_threads = stream->max_threads,
    };

    return 1;

Fail:
    reset_owned_fields(sa);
    return 0;
}

void
shim_array_destroy(struct shim_array* a)
{
    if (!a) {
        return;
    }
    shim_sink_flush(&a->sink);
    shim_sink_destroy(&a->sink);
    free(a->dims);
    a->dims = NULL;
    free(a->axes);
    a->axes = NULL;
    free(a->key);
    a->key = NULL;
}
