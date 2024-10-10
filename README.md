# Acquire Zarr streaming library

[![Build](https://github.com/acquire-project/acquire-zarr/actions/workflows/build.yml/badge.svg)](https://github.com/acquire-project/acquire-zarr/actions/workflows/build.yml)
[![Tests](https://github.com/acquire-project/acquire-zarr/actions/workflows/test_pr.yml/badge.svg)](https://github.com/acquire-project/acquire-zarr/actions/workflows/test_pr.yml)
[![Chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://acquire-imaging.zulipchat.com/)

This library supports chunked, compressed streaming to [Zarr][].

## Dependencies

This library has the following dependencies:
- [c-blosc](https://github.com/Blosc/c-blosc) v1.21.6
- [nlohmann-json](https://github.com/nlohmann/json) v3.11.3
- [minio-cpp](https://github.com/minio/minio-cpp) v0.3.0

We prefer using [vcpkg] for dependency management Below are instructions for installing vcpkg locally and configuring it to fetch and compile the necessary dependencies (the steps are taken from [this vcpkg guide](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started?pivots=shell-bash)).

- ``
- ` `
- Add export commands to your shell's profile script (e.g., `~/.bashrc` or `~/.zshrc`)
  - ``
  - ``
  - 
- Select the default CMake preset before building (consider deleting your build directory first)
  - `cmake --preset=default -B /path/to/build`
    - (Alternatively, from the build directory, run `cmake --preset=default /path/to/source`.)
    - If you're building this project on Windows, you might need to specify your compiler triplet. This ensures that all dependencies are built as static libraries. You can specify the triplet during the preset selection process.
      - `cmake --preset=default -DVCPKG_TARGET_TRIPLET=x64-windows-static ...`

## Building

### Installing dependencies

You should use [vcpkg] to install dependencies, as it integrates well with CMake.

To install vcpkg, clone the repository and bootstrap it:

```bash
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg && ./bootstrap-vcpkg.sh
```

and then add the vcpkg directory to your path:

```bash
cat >> ~/.bashrc <<EOF
export VCPKG_ROOT=${PWD}
export PATH=\$VCPKG_ROOT:\$PATH
EOF
```

If you're using Windows, click [here](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_environment_variables?view=powershell-7.4#set-environment-variables-in-the-system-control-panel) to learn how to set environment variables.
You will need to set both the `VCPKG_ROOT` and `PATH` variables in the system control panel.

### Configuring

To build the library, you can use CMake:

```bash
cmake --preset=default -B /path/to/build /path/to/source
```

On Windows, you'll need to specify the target triplet to ensure that all dependencies are built as static libraries:

```pwsh
cmake --preset=default -B /path/to/build -DVCPKG_TARGET_TRIPLET=x64-windows-static /path/to/source
```

Aside from the usual CMake options, you can choose to disable tests by setting `BUILD_TESTING` to `OFF`:

```bash
cmake --preset=default -B /path/to/build -DBUILD_TESTING=OFF /path/to/source
```

To build the Python bindings, you can set `BUILD_PYTHON` to `ON`:

```bash
cmake --preset=default -B /path/to/build -DBUILD_PYTHON=ON /path/to/source
```

### Building

After configuring, you can build the library:

```bash
cmake --build /path/to/build
```

## Devices

### Storage

- **Zarr**
- **ZarrBlosc1ZstdByteShuffle**
- **ZarrBlosc1Lz4ByteShuffle**
- **ZarrV3**
- **ZarrV3Blosc1ZstdByteShuffle**
- **ZarrV3Blosc1Lz4ByteShuffle**

## Using the Zarr storage device

Zarr has additional capabilities relative to the basic storage devices, namely _chunking_, _compression_, and
_multiscale storage_.

To compress while streaming, you can use one of the `ZarrBlosc1*` devices.
Chunking is configured using `storage_properties_set_chunking_props()` when configuring your video stream.
Multiscale storage can be enabled or disabled by calling `storage_properties_set_enable_multiscale()` when configuring
the video stream.

For the [Zarr v3] version of each device, you can use the `ZarrV3*` devices.
**Note:** Zarr v3 is not [yet](https://github.com/ome/ngff/pull/206) supported
by [ome-zarr-py](https://github.com/ome/ome-zarr-py), so you
will not be able to read multiscale metadata from the resulting dataset.

Zarr v3 *is* supported by [zarr-python](https://github.com/zarr-developers/zarr-python), but you will need to set two
environment variables to work with it:

```bash
export ZARR_V3_EXPERIMENTAL_API=1
export ZARR_V3_SHARDING=1
```

You can also set these variables in your Python script:

```python
import os

# these MUST come before importing zarr
os.environ["ZARR_V3_EXPERIMENTAL_API"] = "1"
os.environ["ZARR_V3_SHARDING"] = "1"

import zarr
```

### Configuring the output array

You will need to specify the shape of the output array when configuring your video stream.
The `StorageProperties` object has a field `acquisition_dimensions`, which is defined like so:

```c
struct storage_properties_dimensions_s
{
    // The dimensions of the output array.
    struct StorageDimension* data;

    // The number of dimensions in the output array.
    size_t size;
};
```

Observe that this struct contains a pointer to a `struct StorageDimension` array, as well as a `size_t` field `size`.

Let's look at the `StorageDimension` struct.
This struct has the following fields:

```c
struct StorageDimension
{
    // the name of the dimension as it appears in the metadata, e.g.,
    // "x", "y", "z", "c", "t"
    struct String name;

    // the type of dimension, e.g., spatial, channel, time
    enum DimensionType kind;

    // the expected size of the full output array along this dimension
    uint32_t array_size_px;

    // the size of a chunk along this dimension
    uint32_t chunk_size_px;

    // the number of chunks in a shard along this dimension
    uint32_t shard_size_chunks;
};
```

Each of your output dimensions should have a corresponding `StorageDimension` struct.
The order of these dimensions matters: the first dimension in the array will be the fastest-varying dimension as you
acquire.
Then the next dimension will be the next-fastest varying, and so on.
The last dimension will be the slowest-varying, i.e., the append dimension.

The first two dimensions should represent the width and height of the frame, respectively.
The `array_size_px` for these dimensions should match the width and height of the frame, and the `kind` field should
be `DimensionType_Space`. The rest of the dimensions should match the order of acquisition.

You can configure chunking and sharding for each dimension by setting the `chunk_size_px` and `shard_size_chunks`
fields, respectively.

In general, you should not manipulate this struct, or the array, directly.
Instead, there are helper functions that can be used to initialize the array and set up each of the storage dimensions,
given a pointer to the `StorageProperties` struct that contains them:

```c
/// Initializes StorageProperties, allocating string storage on the heap
/// and filling out the struct fields.
/// @returns 0 when `bytes_of_out` is not large enough, otherwise 1.
/// @param[out] out The constructed StorageProperties object.
/// @param[in] first_frame_id (unused; aiming for future file rollover
/// support
/// @param[in] filename A c-style null-terminated string. The file to create
///                     for streaming.
/// @param[in] bytes_of_filename Number of bytes in the `filename` buffer
///                              including the terminating null.
/// @param[in] metadata A c-style null-terminated string. Metadata string
///                     to save along side the created file.
/// @param[in] bytes_of_metadata Number of bytes in the `metadata` buffer
///                              including the terminating null.
/// @param[in] pixel_scale_um The pixel scale or size in microns.
/// @param[in] dimension_count The number of dimensions in the storage
/// array. Each of the @p dimension_count dimensions will be initialized
/// to zero.
int storage_properties_init(struct StorageProperties* out,
                            uint32_t first_frame_id,
                            const char* filename,
                            size_t bytes_of_filename,
                            const char* metadata,
                            size_t bytes_of_metadata,
                            struct PixelScale pixel_scale_um,
                            uint8_t dimension_count);

/// @brief Set the value of the StorageDimension struct at index `index` in
/// `out`.
/// @param[out] out The StorageProperties struct containing the
/// StorageDimension array.
/// @param[in] index The index of the dimension to set.
/// @param[in] name The name of the dimension.
/// @param[in] bytes_of_name The number of bytes in the name buffer.
///                          Should include the terminating NULL.
/// @param[in] kind The type of dimension.
/// @param[in] array_size_px The size of the array along this dimension.
/// @param[in] chunk_size_px The size of a chunk along this dimension.
/// @param[in] shard_size_chunks The number of chunks in a shard along this
///                              dimension.
/// @returns 1 on success, otherwise 0
int storage_properties_set_dimension(struct StorageProperties* out,
                                     int index,
                                     const char* name,
                                     size_t bytes_of_name,
                                     enum DimensionType kind,
                                     uint32_t array_size_px,
                                     uint32_t chunk_size_px,
                                     uint32_t shard_size_chunks);
```

You will need to call `storage_properties_init()` _before_ calling `storage_properties_set_dimension()`.
You can find the implementation of these functions in the [acquire-common][] library.

#### Example

Let's define some terms:

| ![frames](https://github.com/aliddell/acquire-driver-zarr/assets/844464/3510d468-4751-4fa0-b2bf-0e29a5f3ea1c) |
|:-------------------------------------------------------------------------------------------------------------:|
|                                            A collection of frames.                                            |

A _tile_ is a contiguous section, or region of interest, of a _frame_.

| ![tiles](https://github.com/aliddell/acquire-driver-zarr/assets/844464/f8d16139-e0ac-44db-855f-2f5ef305c98b) |
|:------------------------------------------------------------------------------------------------------------:|
|                                 A collection of frames, divided into tiles.                                  |

A _chunk_ is nothing more than some number of stacked tiles from subsequent frames, with each tile in a chunk having
the same ROI in its respective frame.

| ![chunks](https://github.com/aliddell/acquire-driver-zarr/assets/844464/653e4d82-363e-4e04-9a42-927b052fb6e7) |
|:-------------------------------------------------------------------------------------------------------------:|
|            A collection of frames, divided into tiles. A single chunk has been highlighted in red.            |

A _shard_ is a collection of chunks within a single file or S3 bucket.

| ![shards](https://user-images.githubusercontent.com/7216331/142424772-fa0b700a-8176-4663-a136-9807e64f7078.png) |
|:---------------------------------------------------------------------------------------------------------------:|
|                         Shards aggregate chunks within individual files or S3 buckets.                          |

Suppose you have a video stream with the following dimensions:

- Width: 1920 px
- Height: 1080 px
- Channels: 3
- Time: 1000 frames (per channel)

You want to divide your frames into 4 x 4 tiles of size 480 x 270, and you want each channel to be stored in separate
chunks.
You also want to aggregate 100 time points into a single chunk.
You will end up with 4 x 4 = 16 chunks of size 480 x 270 x 1 x 100 for each channel, for a total of 48 chunks, for every
100 time points.
Suppose further that you wanted to aggregate each of these chunks into a single *shard*.
Configuring your storage dimensions might look like this:

```cpp
struct StorageProperties props = { 0 };

const char filename[] = "my_video.zarr";
const char external_metadata[] = R"({"my":"metadata"})";
const struct PixelScale sample_spacing_um = { 1, 1 };
storage_properties_init(&props,
                        0, // first frame id
                        (char*)filename,
                        strlen(filename) + 1,
                        (char*)external_metadata,
                        sizeof(external_metadata),
                        sample_spacing_um,
                        4); // number of dimensions

// width
storage_properties_set_dimension(
  &props,
  0,
  "x", // name
  2,   // number of bytes in the name, including the null terminator
  DimensionType_Space, // type of the dimension
  1920,                // full size of the dimension
  480,                 // number of pixels in a chunk
  4); // aggregate all 4 chunks into a shard along this dimension

// height
storage_properties_set_dimension(
  &props,
  1,
  "y", // name
  2,   // number of bytes in the name, including the null terminator
  DimensionType_Space, // type of the dimension
  1080,                // full size of the dimension
  270,                 // number of pixels in a chunk
  4); // aggregate all 4 chunks into a shard along this dimension

// channels
storage_properties_set_dimension(
  &props,
  2
  "c", // name
  2,   // number of bytes in the name, including the null terminator
  DimensionType_Channel, // type of the dimension
  3,                     // full size of the dimension
  1,                     // number of pixels in a chunk (1 channel per chunk)
  1); // one single-channel chunk per shard along this dimension

// time
storage_properties_set_dimension(
  &props,
  3,
  "t", // name
  2,   // number of bytes in the name, including the null terminator
  DimensionType_Time, // type of the dimension
  0,   // append dimension; we don't know the full size a priori
  100, // number of pixels in a chunk
  1);  // one 100-timepoint chunk per shard along this dimension
```

### Compression

Compression is done via [Blosc][].
Supported codecs are **lz4** and **zstd**, which can be used by using the **ZarrBlosc1Lz4ByteShuffle** and
**ZarrBlosc1ZstdByteShuffle** devices, respectively.
For a comparison of these codecs, please refer to the [Blosc docs][].

### Configuring multiscale

In order to enable or disable multiscale storage for your video stream, you can call
`storage_properties_set_enable_multiscale()` on your `StorageProperties` object _after_ calling
`storage_properties_init()` on it.

```c
int
storage_properties_set_enable_multiscale(struct StorageProperties* out,
                                         uint8_t enable);
```

To enable, pass `1` as the `enable` parameter, and to disable, pass `0`.

If enabled, the Zarr writer will write a pyramid of frames, with each level of the pyramid halving each dimension of
the previous level, until the dimensions are less than or equal to a single tile.

#### Example

Suppose your frame size is 1920 x 1080, with a tile size of 384 x 216.
Then the sequence of levels will have dimensions 1920 x 1080, 960 x 540, 480 x 270, and 240 x 135.

[Zarr]: https://zarr.readthedocs.io/en/stable/spec/v2.html

[Blosc]: https://github.com/Blosc/c-blosc

[Blosc docs]: https://www.blosc.org/

[Zarr v3]: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html

[acquire-common]: https://github.com/acquire-project/acquire-common

[vcpkg]: https://vcpkg.io/en/