#include "shim_sink.h"

struct shard_sink*
shim_sink_as_shard_sink(struct shim_sink* s)
{
    switch (s->kind) {
        case SHIM_SINK_ARRAY:
            return zarr_array_as_shard_sink(s->array);
        case SHIM_SINK_MULTISCALE:
            return ngff_multiscale_as_shard_sink(s->multiscale);
        case SHIM_SINK_NONE:
            break;
    }
    return NULL;
}

void
shim_sink_flush(struct shim_sink* s)
{
    switch (s->kind) {
        case SHIM_SINK_ARRAY:
            zarr_array_flush(s->array);
            break;
        case SHIM_SINK_MULTISCALE:
            ngff_multiscale_flush(s->multiscale);
            break;
        case SHIM_SINK_NONE:
            break;
    }
}

void
shim_sink_destroy(struct shim_sink* s)
{
    switch (s->kind) {
        case SHIM_SINK_ARRAY:
            zarr_array_destroy(s->array);
            s->array = NULL;
            break;
        case SHIM_SINK_MULTISCALE:
            ngff_multiscale_destroy(s->multiscale);
            s->multiscale = NULL;
            break;
        case SHIM_SINK_NONE:
            break;
    }
}
