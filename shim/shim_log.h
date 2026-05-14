#pragma once

// Forward the acquire-zarr log level to chucky's log dispatcher. Exposed so
// the stream module can re-apply the level at stream-create time; callers
// that only set the level via the public API do not need to invoke this
// directly.
void
shim_apply_log_level(void);
