[Chucky](https://github.com/acquire-project/chucky) is a replacement for the
acquire-zarr backend. It's fairly feature complete, but constitutes a pretty
big change. It is more performant, adds gpu acceleration, and adds some
flexibility around how levels-of-detail are computed.

Integrating with acquire-zarr will be done in a few steps. First, we'll build
a library that implements acquire-zarr's public c api, and get that running.

That's what this `shim` folder is all about. We'll build that out here, and
depending on how that goes, we may migrate over to the new backend more
permanently.

## Building and testing

The shim builds inside a Docker container (CUDA toolkit required even for the
CPU-only backend because chucky's CMake enables the CUDA language).

> [!NOTE]
> On Windows, Docker must be configured to use Linux containers. Right-click the Docker Desktop system tray icon and select "Switch to Linux containers..." if necessary.

Build and run all tests (filesystem + S3 via MinIO):

```
docker compose -f shim/docker-compose.yml up --build
```

Run a single test:

```
docker compose -f shim/docker-compose.yml run test \
    ctest --test-dir shim/build -R stream-raw-to-filesystem --output-on-failure
```

Tear down:

```
docker compose -f shim/docker-compose.yml down
```

## Library

The shim produces a static library called `acquire-zarr-chucky-cpu`. It
implements all 28 functions from `include/acquire.zarr.h` by translating
acquire-zarr types into chucky's C API and forwarding calls.

This is the CPU-only variant. A GPU variant will follow separately.

## Known differences from the original acquire-zarr

- **Compression codecs**: acquire-zarr wraps lz4/zstd inside Blosc1. Chucky
  uses raw lz4/zstd. The `level` and `shuffle` compression parameters are
  accepted but ignored. The zarr.json codec metadata will differ.
- **Thread count**: `max_threads` is accepted but ignored. Chucky manages its
  own threading via OpenMP.
