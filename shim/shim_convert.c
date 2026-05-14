#include "shim_convert.h"

#include "log/log.h"

#include <stdlib.h>
#include <string.h>

enum dtype
shim_convert_dtype(ZarrDataType dt)
{
    switch (dt) {
        case ZarrDataType_uint8:
            return dtype_u8;
        case ZarrDataType_uint16:
            return dtype_u16;
        case ZarrDataType_uint32:
            return dtype_u32;
        case ZarrDataType_uint64:
            return dtype_u64;
        case ZarrDataType_int8:
            return dtype_i8;
        case ZarrDataType_int16:
            return dtype_i16;
        case ZarrDataType_int32:
            return dtype_i32;
        case ZarrDataType_int64:
            return dtype_i64;
        case ZarrDataType_float32:
            return dtype_f32;
        case ZarrDataType_float64:
            return dtype_f64;
        default:
            return dtype_u8;
    }
}

struct codec_config
shim_convert_codec(const ZarrCompressionSettings* settings)
{
    struct codec_config cfg = { .id = CODEC_NONE,
                                .level = 0,
                                .shuffle = CODEC_SHUFFLE_NONE };
    if (!settings || settings->compressor == ZarrCompressor_None) {
        return cfg;
    }
    cfg.level = settings->level;
    cfg.shuffle = (enum codec_shuffle)settings->shuffle;
    switch (settings->codec) {
        case ZarrCompressionCodec_BloscLZ4:
            cfg.id = CODEC_BLOSC_LZ4;
            break;
        case ZarrCompressionCodec_BloscZstd:
            cfg.id = CODEC_BLOSC_ZSTD;
            break;
        case ZarrCompressionCodec_Zstd:
            cfg.id = CODEC_ZSTD;
            break;
        default:
            // Caller asked for compression with an unrecognized codec id.
            // Fall back to no compression and warn so the silent mismatch
            // is visible.
            log_warn("shim_convert_codec: unknown codec id %d; "
                     "writing uncompressed",
                     (int)settings->codec);
            break;
    }
    return cfg;
}

enum ngff_axis_type
shim_convert_ngff_axis_type(ZarrDimensionType type)
{
    switch (type) {
        case ZarrDimensionType_Space:
            return ngff_axis_space;
        case ZarrDimensionType_Channel:
            return ngff_axis_channel;
        case ZarrDimensionType_Time:
            return ngff_axis_time;
        case ZarrDimensionType_Other:
        default:
            return ngff_axis_space;
    }
}

enum lod_reduce_method
shim_convert_reduce_method(ZarrDownsamplingMethod method)
{
    switch (method) {
        case ZarrDownsamplingMethod_Mean:
            return lod_reduce_mean;
        case ZarrDownsamplingMethod_Min:
            return lod_reduce_min;
        case ZarrDownsamplingMethod_Max:
            return lod_reduce_max;
        case ZarrDownsamplingMethod_Decimate:
            // Chucky has no dedicated decimate reducer. Mean is the closest
            // drop-in; the distinction is silent by design for now.
            return lod_reduce_mean;
        default:
            log_warn("shim_convert_reduce_method: unknown method %d; "
                     "defaulting to mean",
                     (int)method);
            return lod_reduce_mean;
    }
}

struct dimension*
shim_convert_dimensions(const ZarrDimensionProperties* props,
                        size_t count,
                        const size_t* storage_dimension_order,
                        bool multiscale)
{
    struct dimension* dims = calloc(count, sizeof(struct dimension));
    if (!dims) {
        return NULL;
    }

    for (size_t i = 0; i < count; ++i) {
        dims[i].size = props[i].array_size_px;
        dims[i].chunk_size = props[i].chunk_size_px;
        dims[i].chunks_per_shard = props[i].shard_size_chunks;
        dims[i].name = props[i].name;
        dims[i].downsample =
          multiscale && props[i].type == ZarrDimensionType_Space;
        dims[i].storage_position = (uint8_t)i;
    }

    if (storage_dimension_order) {
        for (size_t i = 0; i < count; ++i) {
            dims[storage_dimension_order[i]].storage_position = (uint8_t)i;
        }
    }

    return dims;
}

struct ngff_axis*
shim_convert_ngff_axes(const ZarrDimensionProperties* props, size_t count)
{
    struct ngff_axis* axes = calloc(count, sizeof(struct ngff_axis));
    if (!axes) {
        return NULL;
    }

    for (size_t i = 0; i < count; ++i) {
        axes[i].type = shim_convert_ngff_axis_type(props[i].type);
        axes[i].unit = props[i].unit;
        axes[i].scale = props[i].scale;
    }

    return axes;
}
