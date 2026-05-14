#pragma once

#include "shim_internal.h"

// Create all HCS FOV arrays under `settings->hcs_settings`. Writes the
// plate/row/well group metadata (OME NGFF attributes) and initializes the
// per-FOV multiscale sinks in stream->arrays[*array_idx..]. `*array_idx`
// is advanced past the last FOV written. Returns 1 on success, 0 on
// failure; partial state is cleaned up by ZarrStream_destroy.
int
shim_create_hcs_arrays(struct ZarrStream_s* stream,
                       const ZarrStreamSettings* settings,
                       size_t* array_idx);
