# Shim Implementation Plan

## Current State (2026-04-20)

All 17 integration tests passing (all original acquire-zarr tests ported):
- `stream-raw-to-filesystem` — PASS
- `stream-named-array-to-filesystem` — PASS
- `stream-compressed-to-filesystem` (blosc) — PASS
- `stream-zstd-compressed-to-filesystem` — PASS
- `stream-2d-multiscale-to-filesystem` — PASS
- `stream-3d-multiscale-to-filesystem` — PASS
- `stream-multi-frame-append` — PASS
- `stream-multiscale-trivial-3rd-dim` — PASS
- `stream-multiple-arrays-to-filesystem` — PASS
- `estimate-memory-usage` — PASS
- `stream-pure-hcs-acquisition` — PASS
- `stream-mixed-flat-and-hcs-acquisition` — PASS
- `stream-with-ragged-final-shard` — PASS
- `stream-raw-to-s3` — PASS (via minio in docker-compose)
- `stream-named-array-to-s3` — PASS
- `stream-compressed-to-s3` — PASS
- `stream-append-nullptr` — PASS (tests both filesystem and S3)

Ported shim to chucky's public API (store → zarr_array/ngff_multiscale).
All arrays coordinated by a single `multiarray_tile_stream` (CPU or GPU,
selected at compile time via `shim_backend.h`) with shared pools sized to
the maximum across arrays (constant memory for N arrays).
S3 store support wired via chucky's `store_s3_create` (aws-c-s3).
HCS support fully wired: plate/well/FOV metadata, per-FOV multiscale sinks, data routing.
Logging wired to chucky's public `chucky_log.h` API; Python module routes
events into `logging.getLogger("acquire_zarr")` via a chucky callback and
silences chucky's default stderr sink on import (see divergence #9).

### Multiarray constraint (HCS tests updated)

The multiarray tile stream requires that switching arrays happens at an
**epoch boundary** (so shared buffers can be reused without flushing partial
state). A write of one (y, x) frame to a FOV must equal one epoch:
`epoch_elements = chunks_per_epoch * chunk_elements = frame_size`.

HCS tests updated to use chunk sizes that evenly divide the frame:
`y_chunk=240` (2 chunks over 480), `x_chunk=320` (2 chunks over 640). This
is how production acquisitions should configure chunks.

## Chucky submodule

On main, including GPU multiarray writer (#81), shared-LOD split (#82),
CPU multiarray heap-overflow fixes (#83), the public log header (#87),
the `zarr_write_attribute` API (#88), `store_has_existing_data` (#89),
idempotent multiarray flush (#91), the explicit stream commit point
(#92), the zarr.json write-length clamp (#96/#100, fixes the Windows
UTF-8 decode error), and macOS/Darwin platform support (#99, fixes the
Apple-clang OpenMP wiring). The two local fixes previously listed here
have been upstreamed.

## Architecture

The shim uses chucky's public API:
- **store** (`store_fs_create`) — filesystem key-value store
- **zarr_array** (`zarr_array_create`) — non-multiscale arrays (shard geometry computed internally)
- **ngff_multiscale** (`ngff_multiscale_create`) — multiscale arrays (auto LOD levels, writes NGFF group metadata)
- **multiarray tile stream** — streaming pipeline for N arrays with shared
  pools (chunk tiling, LOD pyramid, compression). CPU via
  `multiarray_tile_stream_cpu`, GPU via `multiarray_tile_stream_gpu`;
  selected at compile time by `shim_backend.h`. Switching between arrays
  only valid at epoch boundaries.

Internal APIs used only where needed:
- `zarr/store.h` — for `store->mkdirs()` in HCS hierarchy and intermediate groups
- `zarr/zarr_group.h` — for `zarr_group_write_with_raw_attrs()`
- `zarr/json_writer.h` — for HCS metadata JSON helpers

HCS is built directly in the shim (not using chucky's `hcs_plate_create`) to support
per-well/per-FOV heterogeneous configs from the acquire-zarr API.

Intermediate groups are written for each path component of array keys.

## Store Layout

- **Non-multiscale, no output_key**: flat at root via `zarr_array`
  - `store.zarr/zarr.json` = array metadata
  - `store.zarr/c/...` = shard data
- **Multiscale**: group + subdirectory via `ngff_multiscale`
  - `store.zarr/zarr.json` = group (OME multiscales)
  - `store.zarr/0/zarr.json` = L0 array
- **Named array (output_key)**: under subdirectory
  - `store.zarr/key/zarr.json` = array or group
  - Intermediate groups written at each path component
- **HCS**: plate → row → well → FOV hierarchy
  - `store.zarr/plate/zarr.json` = plate group with OME plate attributes
  - `store.zarr/plate/row/zarr.json` = row group
  - `store.zarr/plate/row/col/zarr.json` = well group with OME well attributes
  - `store.zarr/plate/row/col/fov/zarr.json` = FOV multiscale group

## Behavioral divergences from baseline acquire-zarr

Documented where the shim-backed library behaves differently than the main
branch implementation. Callers moving from baseline should review these.

### 1. LOD / multiscale geometry

Chucky's LOD rules (implemented via #70, #74, #fef0e1f):
1. All LOD dimensions are halved together at each level
2. Dimensions are clamped at chunk_size: `max((size+1)/2, chunk_size)`
3. **Chunk sizes are constant across all levels** (baseline shrank chunks)
4. Scale = `base_scale * (1 << n_times_downsampled)` per dimension
5. Stopping conditions (first wins):
   - `preserve_aspect_ratio && any LOD dim at chunk_size` (optional)
   - `all LOD dims at chunk_size` (always)
   - `nlod >= max_nlod` (always)
6. Dimensions that reach chunk_size drop from the LOD set for subsequent levels

Integration tests `stream-2d-multiscale`, `stream-3d-multiscale`,
`stream-multiscale-trivial-3rd-dim`, and LOD2 shape in
`stream-multiple-arrays-to-filesystem` were updated to expect this behavior.
Three of those — `stream-2d-multiscale-to-filesystem`,
`stream-3d-multiscale-to-filesystem`, and
`stream-multiple-arrays-to-filesystem` — are **dual-maintained**: the
baseline expectations live at `tests/integration/<name>.cpp` (run by the
baseline CI) and the chucky-LOD expectations live at
`shim/tests/integration/<name>.cpp` (run by the shim CI). Cross-reference
banners at the top of each file point at the sibling. Keep non-LOD changes
mirrored between the two copies.

### 2. Multiarray epoch-boundary constraint

The multiarray tile stream shares chunk/compressed/LUT pools across N arrays
(constant-memory design for 100s–1000s of arrays). Switching the active array
mid-epoch is rejected (`not_flushable`).

Practical requirement: for common "one frame per append" workflows, configure
chunk sizes so that one frame equals one epoch:
`epoch_elements = chunks_per_epoch * chunk_elements = frame_size`. Chunks in
the non-append dims must evenly divide the corresponding array sizes.

Baseline had independent per-array streams and allowed arbitrary interleaved
partial writes. HCS tests updated: `y_chunk=240` / `x_chunk=320` over
`480×640` frames (4 chunks = 1 epoch = 1 frame).

### 3. `ZarrStream_get_current_memory_usage` — upper-bound estimate

Returns a value set once at stream create time from
`ZarrStreamSettings_estimate_max_memory_usage` (extended to walk HCS FOVs
as well as flat arrays). This is an upper bound, not runtime-tracked
usage, since chucky allocates pools once at create and they don't grow.

### 4. `settings->overwrite` — wired

`ZarrStream_create` calls `store_has_existing_data` (#89) immediately
after opening the store. When `settings->overwrite` is false and the
store reports existing data, create fails (logged as "refusing to
overwrite"). A negative return from `store_has_existing_data` (HEAD
failure) also aborts — don't silently treat transient errors as
"absent". Works for both filesystem and S3 backends. Baseline's stricter
"scan and remove" on overwrite=true isn't required since chucky clobbers
per-shard.

### 5. Logging wired to chucky's public API

C API `Zarr_set_log_level` forwards to `chucky_log_set_level` /
`chucky_log_set_quiet` (gates chucky's stderr sink).

Python module registers a `chucky_log_add_callback` at import and calls
`chucky_log_set_quiet(1)` to silence chucky's stderr. Python users
control verbosity via `logging` — `Zarr_set_log_level` still round-trips
but no longer affects output.

Chucky callbacks fire on arbitrary threads (including IO workers that
run while the main thread has released the GIL). Delivering events
straight into Python from the producer thread is a deadlock risk: if
the Python handler blocks (e.g. on pytest's captured-stderr pipe) while
the main thread is waiting on an IO fence, the worker never retires its
job and nothing drains the pipe. The producer-side callback therefore
pushes events into a bounded mutex-protected ring and never touches
Python; a `drain_log_ring()` runs on the Python thread (GIL held) from
an RAII `LogDrainGuard` placed at the top of each bound method, so
events reach Python on both normal and exception-propagating return
paths. Overflow drops oldest silently and reports a count as a
`warning`. See `python/acquire-zarr-py.cpp`.

## CI (shim tests)

`test-shim.yml` runs four jobs:
- **linux**: `docker compose run --rm test` — builds the Docker image
  and runs the full `ctest -L shim` suite with minio for S3. This is
  one of two places S3 tests run (the other is gpu).
- **macos** (macos-latest, arm): native build via micromamba
  (`mamba-org/setup-micromamba@v2`) using the conda-forge packages
  `aws-c-s3 blosc lz4-c zstd llvm-openmp snappy zlib nlohmann_json`.
  Runs `ctest -L shim -LE s3` (S3 skipped; covered by the linux job).
- **windows** (windows-latest): same micromamba pattern + MSVC
  (`ilammy/msvc-dev-cmd@v1`, arch x64); points `CMAKE_PREFIX_PATH` at
  `$CONDA_PREFIX/Library`. Also runs `ctest -L shim -LE s3`.
- **gpu** (self-hosted `[self-hosted, Linux, gpu]`): `docker compose
  run --rm test-gpu` — builds the CUDA 12.8 + nvcomp 5.1 image from
  `shim/Dockerfile.gpu` (`test-build` stage), mounts the GPU via CDI
  (`nvidia.com/gpu=all`), brings up minio, runs the full `ctest -L
  shim` suite against the GPU backend. Mirrors chucky's own gpu-tests
  job.

The macos/windows/gpu jobs mirror chucky's own `ci.yml` pattern so the
two repos stay in sync on platform support.

Note on OpenMP: `shim/CMakeLists.txt` still pre-seeds `FindOpenMP`
variables for Homebrew's keg-only libomp (needed by the benchmark
workflow which `brew install libomp`s). The block is guarded by
`EXISTS ${brew_path} AND NOT DEFINED ENV{CONDA_PREFIX}` so it doesn't
fight with conda's `llvm-openmp`.

## Remaining Work

Nothing outstanding on the shim side. GPU CI was the last nice-to-have
and is now wired.

## Files

```
.github/workflows/
  test-shim.yml           # linux (docker+minio) + macos/windows (micromamba)
  wheels.yml              # cpu-wheel + gpu-wheel jobs, upload artifacts
shim/
  CMakeLists.txt          # builds chucky, shim lib (cpu+gpu), integration tests
  Dockerfile              # CPU build/test + CPU wheel stages
  Dockerfile.gpu          # GPU wheel stages (CUDA 12.8 + nvcomp 5.1)
  docker-compose.yml      # MinIO + test service
  README.md               # build/test docs
  plan.md                 # this file
  shim.c                  # ZarrStream lifecycle + append
  shim_array.h/.c         # flat + multiscale array creation
  shim_backend.h          # preprocessor dispatch — CPU vs GPU backend names
  shim_convert.h/.c       # type conversion (dims, ngff_axes, codec, dtype)
  shim_hcs.h/.c           # HCS plate/well/FOV orchestration
  shim_hcs_json.h/.c      # OME/NGFF plate + well attribute JSON builders
  shim_internal.h         # ZarrStream_s, shim_array layout
  shim_log.h/.c           # Zarr_get_api_version, Zarr_*_log_level, status msgs
  shim_settings.h/.c      # ZarrStreamSettings_* / HCS settings allocators + queries
  shim_sink.h/.c          # discriminated union sink (ARRAY + MULTISCALE + NONE)
  shim_util.h/.c          # alloc_printf + intermediate group writer
  pybind/
    CMakeLists.txt          # pybind11 module linked against selected backend
  python/
    pyproject.toml          # wheel metadata (acquire-zarr-cpu)
    setup.py                # CMake-driven CPU wheel build
  python-gpu/
    pyproject.toml          # wheel metadata (acquire-zarr-gpu)
    setup.py                # CMake-driven GPU wheel build
  compat/
    logger.hh/.cpp/.types.h  # C++ logger for test macro compat
  tests/integration/      # shim-flavoured copies of multiscale tests
  chucky/                 # submodule
```
