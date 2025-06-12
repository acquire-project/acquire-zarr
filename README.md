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

A typical use case for a 4-dimensional acquisition might look like this:

```c
ZarrStreamSettings settings = (ZarrStreamSettings){
    .store_path = "my_stream.zarr",
    .data_type = ZarrDataType_uint16,
    .version = ZarrVersion_3,
    .output_key = "dataset1",  // Optional: path within Zarr where data should be stored
    .overwrite = true,         // Optional: remove existing data at store_path if true
};

ZarrStreamSettings_create_dimension_array(&settings, 4);
settings.dimensions[0] = (ZarrDimensionProperties){
    .name = "t",
    .type = ZarrDimensionType_Time,
    .array_size_px = 0,      // this is the append dimension
    .chunk_size_px = 100,    // 100 time points per chunk
    .shard_size_chunks = 10, // 10 chunks per shard
};

// ... rest of dimensions configuration ...

ZarrStream* stream = ZarrStream_create(&settings);

size_t bytes_written;
ZarrStream_append(stream, my_frame_data, my_frame_size, &bytes_written);
assert(bytes_written == my_frame_size);
```

Look at [acquire.zarr.h](include/acquire.zarr.h) for more details.

This acquisition in Python would look like this:

```python
import acquire_zarr as aqz
import numpy as np

settings = aqz.StreamSettings(
    store_path="my_stream.zarr",
    data_type=aqz.DataType.UINT16,
    version=aqz.ZarrVersion.V3,
    output_key="dataset1",  # Optional: path within Zarr where data should be stored
    overwrite=True  # Optional: remove existing data at store_path if true
)

settings.dimensions = [
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

# Generate some random data: one time point, all channels, full frame
my_frame_data = np.random.randint(0, 2 ** 16, (3, 1080, 1920), dtype=np.uint16)

stream = aqz.ZarrStream(settings)
stream.append(my_frame_data)
```

### Organizing Data within a Zarr Container

The library allows organizing data within a Zarr container through the `output_key` setting.
This feature enables you to:

1. Store multiple datasets in a single Zarr container
2. Create hierarchical data organization
3. Manage related acquisitions within a structured layout

Example of storing multiple acquisitions in a single Zarr container:

```python
import acquire_zarr as aqz
import numpy as np

# Create a Zarr container with multiple datasets
container_path = "experiment.zarr"

# First acquisition - store in "sample1/brightfield"
settings1 = aqz.StreamSettings(
    store_path=container_path,
    data_type=aqz.DataType.UINT16,
    version=aqz.ZarrVersion.V3,
    output_key="sample1/brightfield",
    overwrite=False  # Don't overwrite existing data
)
# ... configure dimensions ...
stream1 = aqz.ZarrStream(settings1)
# ... append data ...

# Second acquisition - store in "sample1/fluorescence"
settings2 = aqz.StreamSettings(
    store_path=container_path,
    data_type=aqz.DataType.UINT16,
    version=aqz.ZarrVersion.V3,
    output_key="sample1/fluorescence",
    overwrite=False  # Don't overwrite existing data
)
# ... configure dimensions ...
stream2 = aqz.ZarrStream(settings2)
# ... append data ...
```

The `output_key` parameter accepts a path-like string using forward slashes as separators.
Intermediate groups will be automatically created in the Zarr hierarchy.
If `output_key` is not specified, data will be written at the root of the Zarr container.
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
