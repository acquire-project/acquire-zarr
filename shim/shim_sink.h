#pragma once

#include "zarr.h"
#include "ngff.h"

struct shard_sink;

enum shim_sink_kind
{
    SHIM_SINK_NONE,
    SHIM_SINK_ARRAY,
    SHIM_SINK_MULTISCALE,
};

struct shim_sink
{
    enum shim_sink_kind kind;
    union
    {
        struct zarr_array* array;
        struct ngff_multiscale* multiscale;
    };
};

struct shard_sink*
shim_sink_as_shard_sink(struct shim_sink* s);

void
shim_sink_flush(struct shim_sink* s);

void
shim_sink_destroy(struct shim_sink* s);
