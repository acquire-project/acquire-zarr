# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

**acquire-zarr** is a C/C++ library (with Python bindings via pybind11) for streaming chunked, compressed, multiscale data to Zarr v3 format with OME-NGFF metadata support. It targets high-throughput scientific data acquisition (e.g., high-content screening). Storage backends include local filesystem and S3-compatible object stores.

## Build

Requires CMake 3.23+, a C++20 compiler, and vcpkg (managed via `VCPKG_ROOT`).

```bash
# Full setup (installs Python env, bootstraps vcpkg, builds everything)
just install -p 3.13

# C/C++ only
just cmake-build

# Clean
just clean
```

Key CMake flags:
- `-DBUILD_PYTHON=ON` — build Python bindings
- `-DBUILD_TESTING=ON` — build test suite (default on)
- `-DBUILD_BENCHMARK=OFF` — benchmarks (default off)
- On Windows: `-DVCPKG_TARGET_TRIPLET=x64-windows-static`

Presets are defined in `CMakePresets.json`; the default preset uses the vcpkg toolchain.

## Tests

**C/C++ tests** (CTest):
```bash
just test-cpp
# or directly:
ctest --test-dir build
```

Tests under label `s3` need an S3-compatible endpoint. CI runs `adobe/s3mock:5.2.3`
(`.github/workflows/test.yml`), mapped to `localhost:9000`, because MinIO images are no
longer freely pullable. The same container works locally. It accepts any credentials,
and a plain PUT creates the bucket:

```bash
docker run -d --name s3mock -p 9000:9090 adobe/s3mock:5.2.3
until curl -s -o /dev/null http://localhost:9000/; do sleep 1; done  # wait for the server
curl -fsS -X PUT http://localhost:9000/acquire-test
export ZARR_S3_ENDPOINT=http://localhost:9000 ZARR_S3_BUCKET_NAME=acquire-test
export AWS_ACCESS_KEY_ID=any AWS_SECRET_ACCESS_KEY=any
```

Without these variables the S3 tests return early and pass without testing anything.

A MinIO server that you already run also works.

**Python tests** (pytest, config in `pyproject.toml`):
```bash
just test
# or directly:
uv run pytest
# single test file:
uv run pytest python/tests/test_stream.py
# single test:
uv run pytest python/tests/test_stream.py::test_name
```

S3 tests need env vars: `ZARR_S3_ENDPOINT`, `ZARR_S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

## Linting / formatting

Pre-commit hooks run automatically on commit; to run manually:
```bash
pre-commit run --all-files
```

- **C/C++**: `clang-format` (Mozilla style, 80-col, config in `.clang-format`) and `clang-tidy` (`.clang-tidy`)
- **Python**: `black` (line-length 79, target 3.9–3.13) and `ruff`

## Before you open a pull request

An agent must not open a pull request, or push to one, before a separate review.

1. Start a new agent with a clean context. Do not give it your reasoning or your summary.
   Give it only the repository, the branch, the base and the purpose of the change.
2. Tell it to verify every claim in the diff against the source: code, comments, docs
   and the PR text. It cites file:line for each finding. It also checks the change
   against the current `main`, because a merged PR can make a claim stale.
3. Fix each blocker and each should-fix, or record in the PR why you did not.
4. Put the reviewer's verdict and the commit it reviewed in the PR description.

A self-review in the same context does not satisfy this rule.

## Architecture

### Public interface

- `include/acquire.zarr.h` — C API (the stable public surface)
- `include/zarr.types.h` — all enums and structs (`ZarrStream*`, `ZarrStreamSettings`, `ZarrArraySettings`, dimension types, compression params, HCS types)
- `python/acquire-zarr-py.cpp` — pybind11 wrapper; `python/acquire_zarr/` holds the Python-side package

### Core streaming pipeline (`src/streaming/`)

| File | Role |
|------|------|
| `zarr.stream.hh/cpp` | `ZarrStream`: top-level manager — accepts frames via `append()`, dispatches to arrays, owns the thread pool and frame queue |
| `array.hh/cpp` | `ArrayBase`/`Array`: manages chunk layout and data for one array within a store |
| `array.dimensions.hh/cpp` | Per-dimension metadata (size, chunk size, shard size, transposition order) |
| `multiscale.array.hh/cpp` | Wraps an `Array` and generates downsampled pyramid levels |
| `downsampler.hh/cpp` | Decimate/mean/min/max downsampling logic |
| `chunk.hh/cpp` + `shard.hh/cpp` | Zarr v3 chunk/shard serialization |
| `frame.queue.hh/cpp` | Thread-safe frame buffer between `append()` caller and writer threads |
| `thread.pool.hh/cpp` | Worker threads that compress and write chunks; jobs return `TaskResult` |
| `parallel.for.hh/cpp` | `parallel_for_reduce`: a process-wide team of up to 4 threads that scatters a frame's tiles into chunk buffers; small frames run serially on the caller |
| `sink.hh/cpp` | Abstract write-sink interface |
| `file.sink.hh/cpp` + `posix/`/`win32/` | Filesystem sink (platform-specific implementation) |
| `s3.sink.hh/cpp` + `s3.client.hh/cpp` | S3 sink backed by the aws-crt-cpp vcpkg package |
| `plate.hh/cpp` | HCS plate/well/field-of-view OME-NGFF metadata |
| `file.handle.hh/cpp` | File handle pool to respect OS open-file limits |
| `zarr.common.hh/cpp` | Shared types, JSON helpers (nlohmann-json) |
| `blosc.compression.params`, `zstd.compression.params` | Compression parameter structs |

### Data flow

1. Caller → `ZarrStream::append(frame, array_key)`
2. Frame pushed onto `FrameQueue` with its frame id
3. A single long-lived thread-pool job (`process_frame_queue_`) pops frames and tiles
   them into chunk buffers; no other pool thread touches the queue
4. Pool workers compress chunks with Blosc or zstd and write them — compression and write
   are one job per chunk, not separate stages. With one pool thread, or after the pool
   stops accepting jobs, the job runs inline on the calling thread
   (`Array::dispatch_chunk_job_`): the frame-queue thread while streaming, or the thread
   that calls `ZarrStream_close` for the last flush, because close stops the pool first
5. Chunks are packed into shards with a crc32c-checksummed index footer, written last
   (`index_location: "end"`, Zarr v3 sharding codec)
6. Shards and metadata (`zarr.json`) are written via the sink abstraction. Array metadata
   is rewritten at each append-dimension shard rollover and at close. How atomic that is
   depends on the sink:
   - POSIX file sink: in place with `pwrite` plus `ftruncate` on the live file, so a
     concurrent reader can observe a torn document (`posix/platform.cpp`)
   - Windows file sink: in place with `WriteFile` plus `SetFileInformationByHandle`
     (`win32/platform.cpp`), with the same exposure
   - S3 sink: one whole-object PUT, which is atomic (`s3.sink.cpp`)

   Intermediate group metadata (root, plate, well and generic parent groups) is written
   once, when the stream is created (`write_intermediate_metadata_`), not at close.
   `ZarrIntermediateGroups_Never` skips generic groups. `ZarrIntermediateGroups_IfMissing`
   writes a generic group only if it is absent, unless `overwrite` is set; on S3 that check
   is a conditional PUT (`If-None-Match: *`). The multiscale group's metadata is still
   written at close (`MultiscaleArray::close_`)
7. For multiscale stores, the frame-queue thread drives the levels in sequence, but the
   tile scatter for each level fans out to the `parallel_for_reduce` team. Chunk
   compression and shard writes go to the thread pool

### Tests layout

- `tests/unit-tests/` — component-level (dimensions, chunks, downsampling, frame queue, etc.)
- `tests/integration/` — end-to-end scenarios (filesystem, S3, multiscale, HCS plates)
- `python/tests/` — Python-binding tests (`test_stream.py`, `test_settings.py`, `test_dimension_transposition.py`)

### Examples and benchmarks

- `examples/` — C and C++ examples; `examples/python/` — Python examples
- `benchmarks/` — `benchmark.py`, `plot_benchmarks.py`