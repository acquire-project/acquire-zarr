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

Tests under label `s3` require a local MinIO instance on `localhost:9000`.

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
| `downsampler.hh/cpp` | Mean/mode/median downsampling logic |
| `chunk.hh/cpp` + `shard.hh/cpp` | Zarr v3 chunk/shard serialization |
| `frame.queue.hh/cpp` | Thread-safe frame buffer between `append()` caller and writer threads |
| `thread.pool.hh/cpp` | Worker threads that compress and write chunks; jobs return `TaskStatus` |
| `sink.hh/cpp` | Abstract write-sink interface |
| `file.sink.hh/cpp` + `posix/`/`win32/` | Filesystem sink (platform-specific implementation) |
| `s3.sink.hh/cpp` + `s3.connection.hh/cpp` | S3 sink backed by minio-cpp submodule |
| `plate.hh/cpp` | HCS plate/well/field-of-view OME-NGFF metadata |
| `file.handle.hh/cpp` | File handle pool to respect OS open-file limits |
| `zarr.common.hh/cpp` | Shared types, JSON helpers (nlohmann-json) |
| `blosc.compression.params`, `zstd.compression.params` | Compression parameter structs |

### Data flow

1. Caller → `ZarrStream::append(frame, array_key)`
2. Frame pushed onto `FrameQueue` with its frame id
3. Worker threads pop frames, compress chunks with Blosc or zstd
4. Compressed chunks assembled into shards (Zarr v3 sharding codec)
5. Shards and metadata (`.zarray`, `zarr.json`) written atomically via the sink abstraction
6. For multiscale stores, downsampled levels are written in parallel

### Tests layout

- `tests/unit-tests/` — component-level (dimensions, chunks, downsampling, frame queue, etc.)
- `tests/integration/` — end-to-end scenarios (filesystem, S3, multiscale, HCS plates)
- `python/tests/` — Python-binding tests (`test_stream.py`, `test_settings.py`, `test_dimension_transposition.py`)

### Examples and benchmarks

- `examples/` — C and C++ examples; `examples/python/` — Python examples
- `benchmarks/` — `benchmark.py`, `plot_benchmarks.py`