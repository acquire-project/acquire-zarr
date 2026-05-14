#include "shim_internal.h"
#include "shim_array.h"
#include "shim_hcs.h"
#include "shim_log.h"
#include "log/log.h"
#include "multiarray/multiarray.h"
#include "util/prelude.h"
#include "writer.h"
#include "zarr/store.h"
#include "zarr/store_fs.h"
#include "zarr/zarr_group.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

/* --- Stream lifecycle ---------------------------------------------------- */


ZarrStream*
ZarrStream_create(ZarrStreamSettings* settings)
{
    if (!settings || !settings->store_path) {
        return NULL;
    }
    // Need at least flat arrays or HCS settings
    if (!settings->arrays && !settings->hcs_settings) {
        return NULL;
    }

    // Make sure chucky's log threshold matches the requested level even if
    // the caller never called Zarr_set_log_level.
    shim_apply_log_level();

    ZarrStream* stream = calloc(1, sizeof(ZarrStream));
    if (!stream) {
        return NULL;
    }

    stream->store_path = strdup(settings->store_path);
    if (!stream->store_path) {
        free(stream);
        return NULL;
    }

    CHECK(fail, settings->max_threads <= (unsigned)INT_MAX);
    stream->max_threads = (int)settings->max_threads;

    // Upper-bound memory estimate (same formula as the pre-create estimator;
    // no runtime tracking — allocations happen once at create and don't grow).
    {
        size_t usage = 0;
        (void)ZarrStreamSettings_estimate_max_memory_usage(settings, &usage);
        stream->estimated_memory = usage;
    }

    // Create store
    if (settings->s3_settings) {
        struct store_s3_config s3cfg = {
            .bucket = settings->s3_settings->bucket_name,
            .prefix = settings->store_path,
            .region = settings->s3_settings->region
                        ? settings->s3_settings->region
                        : "us-east-1",
            .endpoint = settings->s3_settings->endpoint,
        };
        store_s3_config_set_defaults(&s3cfg);
        stream->store = store_s3_create(&s3cfg);
    } else {
        stream->store = store_fs_create(settings->store_path, 1);
    }
    if (!stream->store) {
        goto fail;
    }

    // Refuse to overwrite existing data unless the caller opted in. The
    // public API has no error-code return for create, so the caller only
    // sees NULL — the log line documents why.
    // `store_has_existing_data` returns 1=exists, 0=absent, -1=error.
    // A transient HEAD failure shouldn't masquerade as "exists".
    if (!settings->overwrite) {
        int existing = store_has_existing_data(stream->store);
        if (existing > 0) {
            log_error("refusing to overwrite existing data at %s "
                      "(set settings.overwrite=true to replace)",
                      settings->store_path);
            goto fail;
        }
        if (existing < 0) {
            log_error("could not check for existing data at %s "
                      "(store HEAD failed); aborting stream create",
                      settings->store_path);
            goto fail;
        }
    }

    // Ensure the filesystem store root exists; no-op on S3. store_fs_create
    // does not mkdir the root on its own. Empty key yields "<root>/" via
    // snprintf("%s/%s",root,key), which platform_mkdirp creates as <root>.
    stream->store->mkdirs(stream->store, "");

    // Count total arrays
    size_t total_arrays = ZarrStreamSettings_get_array_count(settings);
    stream->n_arrays = total_arrays;
    stream->arrays = calloc(total_arrays, sizeof(struct shim_array));
    if (!stream->arrays && total_arrays > 0) {
        goto fail;
    }

    // Write root group
    if (zarr_group_write_with_raw_attrs(stream->store, "zarr.json", "{}") !=
        0) {
        log_error("failed to write root group zarr.json");
        goto fail;
    }

    // Create flat arrays
    for (size_t i = 0; i < settings->array_count; ++i) {
        if (!shim_create_flat_array(
              stream, &settings->arrays[i], &stream->arrays[i])) {
            goto fail;
        }
    }

    // Create HCS arrays
    if (settings->hcs_settings) {
        size_t array_idx = settings->array_count;
        if (!shim_create_hcs_arrays(stream, settings, &array_idx)) {
            goto fail;
        }
    }

    // Build configs[] and sinks[] for the multiarray stream.
    if (stream->n_arrays > 0) {
        struct tile_stream_configuration* configs =
          calloc(stream->n_arrays, sizeof(struct tile_stream_configuration));
        struct shard_sink** sinks =
          calloc(stream->n_arrays, sizeof(struct shard_sink*));
        if (!configs || !sinks) {
            free(configs);
            free(sinks);
            goto fail;
        }

        for (size_t i = 0; i < stream->n_arrays; ++i) {
            configs[i] = stream->arrays[i].config;
            sinks[i] = shim_sink_as_shard_sink(&stream->arrays[i].sink);
            if (!sinks[i]) {
                free(configs);
                free(sinks);
                goto fail;
            }
        }

        stream->multi_stream = multiarray_tile_stream_create(
          (int)stream->n_arrays, configs, sinks, 0);
        free(configs);
        free(sinks);
        if (!stream->multi_stream) {
            goto fail;
        }
        stream->writer = multiarray_tile_stream_writer(stream->multi_stream);
        if (!stream->writer) {
            goto fail;
        }
    }

    return stream;

fail:
    ZarrStream_destroy(stream);
    return NULL;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    if (!stream) {
        return;
    }
    if (stream->writer) {
        stream->writer->flush(stream->writer);
        stream->writer = NULL;
    }
    if (stream->multi_stream) {
        multiarray_tile_stream_destroy(stream->multi_stream);
        stream->multi_stream = NULL;
    }
    if (stream->arrays) {
        for (size_t i = 0; i < stream->n_arrays; ++i) {
            shim_array_destroy(&stream->arrays[i]);
        }
        free(stream->arrays);
    }
    if (stream->store) {
        stream->store->destroy(stream->store);
    }
    free(stream->store_path);
    free(stream);
}

ZarrStatusCode
ZarrStream_append(ZarrStream* stream,
                  const void* data,
                  size_t bytes_in,
                  size_t* bytes_out,
                  const char* key)
{
    if (!stream || !bytes_out) {
        return ZarrStatusCode_InvalidArgument;
    }

    *bytes_out = 0;

    if (bytes_in == 0) {
        return ZarrStatusCode_Success;
    }

    if (!stream->writer) {
        return ZarrStatusCode_InternalError;
    }

    // Find the target array index. Strict: NULL key only matches when there
    // is exactly one array (typosafe — a stray key won't silently land in
    // the sole unnamed array).
    int array_index = -1;
    if (!key) {
        if (stream->n_arrays == 1) {
            array_index = 0;
        }
    } else {
        for (size_t i = 0; i < stream->n_arrays; ++i) {
            if (stream->arrays[i].key &&
                strcmp(stream->arrays[i].key, key) == 0) {
                array_index = (int)i;
                break;
            }
        }
    }

    if (array_index < 0) {
        return key ? ZarrStatusCode_KeyNotFound : ZarrStatusCode_InvalidArgument;
    }

    // NULL data means "write zeros". Chucky has no fast zero path, so we
    // stream zeros from a small static buffer instead of allocating a full
    // zero frame (frames can be multi-GB). Read-only const bss is
    // thread-safe.
    static const char zero_buf[4096] = { 0 };

    size_t remaining = bytes_in;
    ZarrStatusCode rc = ZarrStatusCode_Success;

    while (remaining > 0) {
        const char* slice_beg;
        size_t slice_len;
        if (data) {
            slice_beg = (const char*)data + (bytes_in - remaining);
            slice_len = remaining;
        } else {
            slice_beg = zero_buf;
            slice_len =
              remaining < sizeof(zero_buf) ? remaining : sizeof(zero_buf);
        }
        struct slice s = { .beg = slice_beg, .end = slice_beg + slice_len };
        struct multiarray_writer_result r =
          stream->writer->update(stream->writer, array_index, s);

        const char* rest_beg = (const char*)r.rest.beg;
        size_t consumed =
          rest_beg ? (size_t)(rest_beg - slice_beg) : slice_len;

        if (r.error == multiarray_writer_finished) {
            // Chucky returns `finished` both for natural completion and for
            // post-capacity appends (as a silent no-op). Distinguish: if the
            // writer failed to consume the full input, the caller tried to
            // write past the array's capacity.
            remaining -= consumed;
            if (remaining > 0) {
                rc = ZarrStatusCode_WriteOutOfBounds;
            }
            break;
        }
        if (r.error == multiarray_writer_not_flushable) {
            // The caller tried to switch to this array while the previously
            // active array is mid-epoch. That's a programming error — the
            // multi-array writer shares chunk pools across arrays and can
            // only switch at epoch boundaries. Report it distinctly so the
            // caller can diagnose.
            const char* k = key ? key : "(no key)";
            log_error("ZarrStream_append: cannot switch to array '%s' "
                      "mid-epoch of a different array; finish the current "
                      "epoch first",
                      k);
            rc = ZarrStatusCode_InvalidArgument;
            break;
        }
        if (r.error != multiarray_writer_ok) {
            log_error("ZarrStream_append: writer error %d", r.error);
            rc = ZarrStatusCode_InternalError;
            break;
        }
        if (consumed == 0) {
            // Writer reported ok without advancing — guard against a spin.
            rc = ZarrStatusCode_InternalError;
            break;
        }
        remaining -= consumed;
    }

    *bytes_out = bytes_in - remaining;
    return rc;
}

ZarrStatusCode
ZarrStream_write_custom_metadata(ZarrStream* stream,
                                 const char* array_key,
                                 const char* metadata_key,
                                 const char* metadata)
{
    if (!stream || !metadata_key || !metadata) {
        return ZarrStatusCode_InvalidArgument;
    }

    // Find the target array by key. NULL array_key selects the single
    // root-level array (only valid when there is exactly one flat array
    // without an output_key).
    struct shim_array* target = NULL;
    for (size_t i = 0; i < stream->n_arrays; ++i) {
        struct shim_array* sa = &stream->arrays[i];
        if (array_key == NULL) {
            if (sa->key == NULL) {
                target = sa;
                break;
            }
        } else if (sa->key && strcmp(sa->key, array_key) == 0) {
            target = sa;
            break;
        }
    }

    if (!target) {
        return ZarrStatusCode_KeyNotFound;
    }

    int rc;
    switch (target->sink.kind) {
        case SHIM_SINK_ARRAY:
            rc = zarr_array_set_attribute(
              target->sink.array, metadata_key, metadata);
            break;
        case SHIM_SINK_MULTISCALE:
            rc = ngff_multiscale_set_attribute(
              target->sink.multiscale, metadata_key, metadata);
            break;
        default:
            return ZarrStatusCode_InternalError;
    }

    return rc == 0 ? ZarrStatusCode_Success : ZarrStatusCode_InternalError;
}

ZarrStatusCode
ZarrStream_get_current_memory_usage(const ZarrStream* stream, size_t* usage)
{
    if (!stream || !usage) {
        return ZarrStatusCode_InvalidArgument;
    }
    *usage = stream->estimated_memory;
    return ZarrStatusCode_Success;
}
