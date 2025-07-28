# acquire-zarr Examples

This directory contains example programs demonstrating how to use the acquire-zarr library for high-speed streaming of bioimaging data to the filesystem or S3 in Zarr format.

## Prerequisites

- CMake 3.23 or later
- vcpkg package manager
- Visual Studio 2019 or later (Windows)

## Building the Examples

### 1. Install Dependencies

The examples use vcpkg to manage dependencies. Make sure you have vcpkg installed and the `VCPKG_ROOT` environment variable set.

From this directory, run:

```bash
vcpkg install
```

### 2. Configure and Build

Using the provided CMake preset:

```bash
cmake --preset=default -DCMAKE_PREFIX_PATH=/path/to/acquire-zarr/install
cmake --build build
```

Or manually:

```bash
cmake -B build -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake -DCMAKE_PREFIX_PATH=/path/to/acquire-zarr/install
cmake --build build
```

Replace `/path/to/acquire-zarr/install` with the actual path to your acquire-zarr installation.
On Windows, you will need to specify the target triplet with `-DVCPKG_TARGET_TRIPLET=x64-windows-static` or similar, depending on your configuration.

## Examples Overview

### Filesystem Storage

- **zarrv3-raw-filesystem.c** - Basic Zarr v3 array writing to filesystem without compression
- **zarrv3-compressed-filesystem.c** - Zarr v3 array with LZ4 compression to filesystem  
- **zarrv3-raw-multiscale-filesystem.c** - Multi-resolution Zarr v3 arrays to filesystem

### S3 Storage

- **zarrv3-raw-s3.c** - Basic Zarr v3 array writing to S3 without compression
- **zarrv3-compressed-s3.c** - Zarr v3 array with ZStd compression to S3
- **zarrv3-compressed-multiscale-s3.c** - Multi-resolution Zarr v3 arrays with compression to S3

## Running the Examples

After building, executables will be in the `build/` directory. Most examples require command-line arguments for configuration:

```bash
# Example usage (adjust paths as needed)
./build/zarrv3-raw-filesystem
./build/zarrv3-compressed-s3
```

You will need to edit your source files to set your output directory and/or S3 configuration.

## S3 Configuration

For S3 examples, you'll need to configure your AWS credentials and endpoint. This can be done through:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
- AWS credentials file

See the individual S3 example source files for endpoint configuration details.

## Troubleshooting

**CMake can't find acquire-zarr**: Make sure `CMAKE_PREFIX_PATH` points to the directory containing `lib/cmake/acquire-zarr/`.

**Missing dependencies**: Ensure vcpkg has installed all required packages and that `VCPKG_ROOT` is set correctly.

**Runtime library mismatch (Windows)**: The examples are configured to use the static runtime library. If you get linking errors, ensure your acquire-zarr library was built with the same runtime library settings.