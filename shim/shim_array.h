#pragma once

#include "shim_internal.h"

// Configure `sa` as a multiscale array: builds dims/axes, creates the
// ngff_multiscale sink under `sa->key`, and fills the tile_stream config.
// `sa->key` must be set by the caller (NULL == root). Returns 1 on success,
// 0 on failure; partial state must be cleaned up by the caller via
// shim_array_destroy.
int
shim_configure_multiscale_array(struct ZarrStream_s* stream,
                                const ZarrArraySettings* as,
                                struct shim_array* sa);

// Create a flat array (non-HCS). Handles both multiscale (via
// shim_configure_multiscale_array) and non-multiscale sinks; wires intermediate
// groups, leaf mkdirs, and the tile_stream config.
int
shim_create_flat_array(struct ZarrStream_s* stream,
                       const ZarrArraySettings* as,
                       struct shim_array* sa);

// Release all owned state of `a` (sink flush+destroy, dims/axes/key free).
// Safe to call on a zero-initialized shim_array.
void
shim_array_destroy(struct shim_array* a);
