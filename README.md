[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14828040.svg)](https://doi.org/10.5281/zenodo.14828040)

# Acquire Zarr streaming library

[![Build](https://github.com/acquire-project/acquire-zarr/actions/workflows/build.yml/badge.svg)](https://github.com/acquire-project/acquire-zarr/actions/workflows/build.yml)
[![Tests](https://github.com/acquire-project/acquire-zarr/actions/workflows/test.yml/badge.svg)](https://github.com/acquire-project/acquire-zarr/actions/workflows/test_pr.yml)
[![Chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://acquire-imaging.zulipchat.com/)
[![PyPI - Version](https://img.shields.io/pypi/v/acquire-zarr)](https://pypi.org/project/acquire-zarr/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/acquire-zarr)](https://pypistats.org/packages/acquire-zarr)

This library supports chunked, compressed, multiscale streaming to [Zarr][], both [version 2][] and [version 3][], with
[OME-NGFF metadata].

This code builds targets for Python and C.

**Note:** Zarr Version 2 is deprecated and will be removed in a future release.
We recommend using Zarr Version 3 for new projects.

## Installing

### Precompiled binaries

C headers and precompiled binaries are available for Windows, Mac, and Linux on
our [releases page](https://github.com/acquire-project/acquire-zarr/releases).

### Python

The library is available on PyPI and can be installed using pip:

```bash
pip install acquire-zarr
```

## Building

### Installing dependencies

This library has the following dependencies:

- [c-blosc](https://github.com/Blosc/c-blosc) v1.21.5
- [nlohmann-json](https://github.com/nlohmann/json) v3.11.3
- [minio-cpp](https://github.com/minio/minio-cpp) v0.3.0
- [crc32c](https://github.com/google/crc32c) v1.1.2

We use [vcpkg] to install them, as it integrates well with CMake.
To install vcpkg, clone the repository and bootstrap it:

```bash
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg && ./bootstrap-vcpkg.sh
```

and then add the vcpkg directory to your path. If you are using `bash`, you can do this by running the following snippet
from the `vcpkg/` directory:

```bash
cat >> ~/.bashrc <<EOF
export VCPKG_ROOT=${PWD}
export PATH=\$VCPKG_ROOT:\$PATH
EOF
```

If you're using Windows, learn how to set environment
variables [here](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_environment_variables?view=powershell-7.4#set-environment-variables-in-the-system-control-panel).
You will need to set both the `VCPKG_ROOT` and `PATH` variables in the system control panel.

On the Mac, you will also need to install OpenMP using Homebrew:

```bash
brew install libomp
```

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

To build the Python bindings, make sure `pybind11` is installed. Then, you can set `BUILD_PYTHON` to `ON`:

```bash
cmake --preset=default -B /path/to/build -DBUILD_PYTHON=ON /path/to/source
```

### Building

After configuring, you can build the library:

```bash
cmake --build /path/to/build
```

### Installing for Python

To install the Python bindings, you can run:

```bash
pip install .
```

> [!NOTE]
> It is highly recommended to use virtual environments for Python, e.g. using `venv` or `conda`. In this case, make sure
> `pybind11` is installed in this environment, and that the environment is activated before installing the bindings.

## Usage

The library provides two main interfaces.
First, `ZarrStream`, representing an output stream to a Zarr dataset.
Second, `ZarrStreamSettings` to configure a Zarr stream.

A typical use case for a single-array, 4-dimensional acquisition might look like this:

```c
ZarrArraySettings array{
    .output_key =
      "my-array", // Optional: path within Zarr where data should be stored
    .data_type = ZarrDataType_uint16,
};

ZarrArraySettings_create_dimension_array(&array, 4);
array.dimensions[0] = (ZarrDimensionProperties){
    .name = "t",
    .type = ZarrDimensionType_Time,
    .array_size_px = 0,      // this is the append dimension
    .chunk_size_px = 100,    // 100 time points per chunk
    .shard_size_chunks = 10, // 10 chunks per shard
};

// ... rest of dimensions configuration ...

ZarrStreamSettings settings = (ZarrStreamSettings){
    .store_path = "my_stream.zarr",
    .version = ZarrVersion_3,
    .overwrite = true, // Optional: remove existing data at store_path if true
    .arrays = &array,
    .array_count = 1, // Number of arrays in the stream
};

ZarrStream* stream = ZarrStream_create(&settings);

// You can now safely free the dimensions array
ZarrArraySettings_destroy_dimension_array(&array);

size_t bytes_written;
ZarrStream_append(stream,
                  my_frame_data,
                  my_frame_size,
                  &bytes_written,
                  "my-array"); // if you have just one array configured, this can be NULL
assert(bytes_written == my_frame_size);
```

Look at [acquire.zarr.h](include/acquire.zarr.h) for more details.

This acquisition in Python would look like this:

```python
import acquire_zarr as aqz
import numpy as np

settings = aqz.StreamSettings(
    store_path="my_stream.zarr",
    version=aqz.ZarrVersion.V3,
    overwrite=True  # Optional: remove existing data at store_path if true
)

settings.arrays = [
    aqz.ArraySettings(
        output_key="array1",
        data_type=aqz.DataType.UINT16,
        dimensions = [
            aqz.Dimension(
                name="t",
                type=aqz.DimensionType.TIME,
                array_size_px=0,
                chunk_size_px=100,
                shard_size_chunks=10
            ),
            aqz.Dimension(
                name="c",
                type=aqz.DimensionType.CHANNEL,
                array_size_px=3,
                chunk_size_px=1,
                shard_size_chunks=1
            ),
            aqz.Dimension(
                name="y",
                type=aqz.DimensionType.SPACE,
                array_size_px=1080,
                chunk_size_px=270,
                shard_size_chunks=2
            ),
            aqz.Dimension(
                name="x",
                type=aqz.DimensionType.SPACE,
                array_size_px=1920,
                chunk_size_px=480,
                shard_size_chunks=2
            )
        ]
    )
]

# Generate some random data: one time point, all channels, full frame
my_frame_data = np.random.randint(0, 2 ** 16, (3, 1080, 1920), dtype=np.uint16)

stream = aqz.ZarrStream(settings)
stream.append(my_frame_data)

# ... append more data as needed ...

# When done, close the stream to flush any remaining data
stream.close()
```

### Organizing data within a Zarr container

The library allows you to stream multiple arrays to a single Zarr dataset by configuring multiple arrays.
For example, a multichannel acquisition with both brightfield and fluorescence channels might look like this:

```python
import acquire_zarr as aqz
import numpy as np

# configure the stream with two arrays
settings = aqz.StreamSettings(
    store_path="experiment.zarr",
    version=aqz.ZarrVersion.V3,
    overwrite=True,  # Remove existing data at store_path if true
    arrays=[
        aqz.ArraySettings(
            output_key="sample1/brightfield",
            data_type=aqz.DataType.UINT16,
            dimensions=[
                aqz.Dimension(
                    name="t",
                    type=aqz.DimensionType.TIME,
                    array_size_px=0,
                    chunk_size_px=100,
                    shard_size_chunks=1
                ),
                aqz.Dimension(
                    name="c",
                    type=aqz.DimensionType.CHANNEL,
                    array_size_px=1,
                    chunk_size_px=1,
                    shard_size_chunks=1
                ),
                aqz.Dimension(
                    name="y",
                    type=aqz.DimensionType.SPACE,
                    array_size_px=1080,
                    chunk_size_px=270,
                    shard_size_chunks=2
                ),
                aqz.Dimension(
                    name="x",
                    type=aqz.DimensionType.SPACE,
                    array_size_px=1920,
                    chunk_size_px=480,
                    shard_size_chunks=2
                )
            ]
        ),
        aqz.ArraySettings(
            output_key="sample1/fluorescence",
            data_type=aqz.DataType.UINT16,
            dimensions=[
                aqz.Dimension(
                    name="t",
                    type=aqz.DimensionType.TIME,
                    array_size_px=0,
                    chunk_size_px=100,
                    shard_size_chunks=1
                ),
                aqz.Dimension(
                    name="c",
                    type=aqz.DimensionType.CHANNEL,
                    array_size_px=2,  # two fluorescence channels
                    chunk_size_px=1,
                    shard_size_chunks=1
                ),
                aqz.Dimension(
                    name="y",
                    type=aqz.DimensionType.SPACE,
                    array_size_px=1080,
                    chunk_size_px=270,
                    shard_size_chunks=2
                ),
                aqz.Dimension(
                    name="x",
                    type=aqz.DimensionType.SPACE,
                    array_size_px=1920,
                    chunk_size_px=480,
                    shard_size_chunks=2
                )
            ]
        )
    ]
)

stream = aqz.ZarrStream(settings)

# ... append data ...
stream.append(brightfield_frame_data, key="sample1/brightfield")
stream.append(fluorescence_frame_data, key="sample1/fluorescence")

# ... append more data as needed ...

# When done, close the stream to flush any remaining data
stream.close()
```

The `overwrite` parameter controls whether existing data at the `store_path` is removed.
When set to `true`, the entire directory specified by `store_path` will be removed if it exists.
When set to `false`, the stream will use the existing directory if it exists, or create a new one if it doesn't.

### S3

The library supports writing directly to S3-compatible storage.
We authenticate with S3 through environment variables or an AWS credentials file.
If you are using environment variables, set the following:

- `AWS_ACCESS_KEY_ID`: Your AWS access key
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret key
- `AWS_SESSION_TOKEN`: Optional session token for temporary credentials

These must be set in the environment where your application runs.

**Important Note:** You should ensure these environment variables are set *before* running your application or importing
the library or Python module.
They will not be available if set after the library is loaded.
Configuration requires specifying the endpoint, bucket
name, and region:

```c
// ensure your environment is set up for S3 access before running your program
#include <acquire.zarr.h>

ZarrStreamSettings settings = { /* ... */ };

// Configure S3 storage
ZarrS3Settings s3_settings = {
    .endpoint = "https://s3.amazonaws.com",
    .bucket_name = "my-zarr-data",
    .region = "us-east-1"
};

settings.s3_settings = &s3_settings;
```

In Python, S3 configuration looks like:

```python
# ensure your environment is set up for S3 access before importing acquire_zarr
import acquire_zarr as aqz

settings = aqz.StreamSettings()
# ...

# Configure S3 storage
s3_settings = aqz.S3Settings(
    endpoint="s3.amazonaws.com",
    bucket_name="my-zarr-data",
    region="us-east-1"
)

# Apply S3 settings to your stream configuration
settings.s3 = s3_settings
```

### Anaconda GLIBCXX issue

If you encounter the error `GLIBCXX_3.4.30 not found` when working with the library in Python, it may be due to a
mismatch between the version of `libstdc++` that ships with Anaconda and the one used by acquire-zarr. This usually
manifests like so:

```
ImportError: /home/eggbert/anaconda3/envs/myenv/lib/python3.10/site-packages/acquire_zarr/../../../lib/libstdc++.so.6: version `GLIBCXX_3.4.30` not found (required by /home/eggbert/anaconda3/envs/myenv/lib/python3.10/site-packages/acquire_zarr/../../../lib/libacquire_zarr.so)
```

To resolve this, you can [install](https://stackoverflow.com/questions/48453497/anaconda-libstdc-so-6-version-glibcxx-3-4-20-not-found/73101774#73101774) the `libstdcxx-ng` package from conda-forge:

```bash
conda install -c conda-forge libstdcxx-ng
```

[Zarr]: https://zarr.dev/

[version 2]: https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html

[version 3]: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html

[Blosc]: https://github.com/Blosc/c-blosc

[vcpkg]: https://vcpkg.io/en/

[OME-NGFF metadata]: https://ngff.openmicroscopy.org/latest/
