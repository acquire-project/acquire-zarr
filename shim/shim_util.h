#pragma once

struct store;

// printf into a freshly-allocated buffer sized to the formatted length.
// Returns NULL on allocation failure. Caller frees.
char*
shim_alloc_printf(const char* fmt, ...);

// Write intermediate group zarr.json for each path component of key.
// For key "a/b/c", writes groups at "a/zarr.json" and "a/b/zarr.json".
// Returns 0 on success, non-zero on allocation or store failure.
int
shim_write_intermediate_groups(struct store* store, const char* key);
