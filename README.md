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
        data_type=np.uint16,
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
            data_type=np.uint16,
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
            data_type=np.uint16,
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

### High-content screening workflows

The library supports high-content screening (HCS) datasets following the [OME-NGFF 0.5](https://ngff.openmicroscopy.org/0.5/) specification.
HCS data is organized into plates, wells, and fields of view, with automatic generation of appropriate metadata.

**Note:** HCS is *not* supported for Zarr V2.

Here's an example of creating an HCS dataset in Python:

```python
import acquire_zarr as aqz
import numpy as np

# Configure field of view arrays
fov1_array = aqz.ArraySettings(
    output_key="fov1",  # Relative to the well: plate/A/1/fov1
    data_type=np.uint16,
    dimensions=[
        aqz.Dimension(
            name="t",
            type=aqz.DimensionType.TIME,
            array_size_px=0,
            chunk_size_px=10,
            shard_size_chunks=1
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
            array_size_px=512,
            chunk_size_px=256,
            shard_size_chunks=2
        ),
        aqz.Dimension(
            name="x",
            type=aqz.DimensionType.SPACE,
            array_size_px=512,
            chunk_size_px=256,
            shard_size_chunks=2
        )
    ]
)

# Create acquisition metadata
acquisition = aqz.Acquisition(
    id=0,
    name="Measurement_01",
    start_time=1343731272000,  # Unix timestamp in milliseconds
    end_time=1343737645000
)

# Configure wells with fields of view
well_a1 = aqz.Well(
    row_name="A",
    column_name="1",
    images=[
        aqz.FieldOfView(
            path="fov1",
            acquisition_id=0,
            array_settings=fov1_array
        )
    ]
)

# Configure the plate
plate = aqz.Plate(
    path="experiment_plate",
    name="My HCS Experiment",
    row_names=["A", "B", "C", "D", "E", "F", "G", "H"],
    column_names=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
    wells=[well_a1],  # Add more wells as needed
    acquisitions=[acquisition]
)

# Create stream with HCS configuration
settings = aqz.StreamSettings(
    store_path="hcs_experiment.zarr",
    version=aqz.ZarrVersion.V3,
    overwrite=True,
    hcs_plates=[plate]
)

stream = aqz.ZarrStream(settings)

# Write data to specific field of view
frame_data = np.random.randint(0, 2**16, (3, 512, 512), dtype=np.uint16)
stream.append(frame_data, key="experiment_plate/A/1/fov1")

# Close when done
stream.close()
```

You can also combine HCS plates with flat arrays in the same dataset:

```python
# Add a labels array alongside HCS data
labels_array = aqz.ArraySettings(
    output_key="experiment_plate/A/1/labels",
    data_type=np.uint8,
    dimensions=[
        aqz.Dimension(
            name="y",
            type=aqz.DimensionType.SPACE,
            array_size_px=512,
            chunk_size_px=256,
            shard_size_chunks=2
        ),
        aqz.Dimension(
            name="x",
            type=aqz.DimensionType.SPACE,
            array_size_px=512,
            chunk_size_px=256,
            shard_size_chunks=2
        )
    ]
)

settings = aqz.StreamSettings(
    store_path="mixed_experiment.zarr",
    version=aqz.ZarrVersion.V3,
    overwrite=True,
    arrays=[labels_array],  # Flat arrays
    hcs_plates=[plate]      # HCS structure
)

stream = aqz.ZarrStream(settings)

# Write to both HCS and flat arrays
stream.append(frame_data, key="experiment_plate/A/1/fov1")
labels_data = np.zeros((512, 512), dtype=np.uint8)
stream.append(labels_data, key="experiment_plate/A/1/labels")

stream.close()
```

In C, the equivalent HCS workflow would look like this:

```c
#include "acquire.zarr.h"

// Create array settings for field of view
ZarrArraySettings fov_array = {
    .output_key = "fov1",  // Relative to well: plate/A/1/fov1
    .data_type = ZarrDataType_uint16,
};

ZarrArraySettings_create_dimension_array(&fov_array, 4);
fov_array.dimensions[0] = (ZarrDimensionProperties){
    .name = "t",
    .type = ZarrDimensionType_Time,
    .array_size_px = 0,
    .chunk_size_px = 10,
    .shard_size_chunks = 1,
};
fov_array.dimensions[1] = (ZarrDimensionProperties){
    .name = "c", 
    .type = ZarrDimensionType_Channel,
    .array_size_px = 3,
    .chunk_size_px = 1,
    .shard_size_chunks = 1,
};
fov_array.dimensions[2] = (ZarrDimensionProperties){
    .name = "y",
    .type = ZarrDimensionType_Space,
    .array_size_px = 512,
    .chunk_size_px = 256,
    .shard_size_chunks = 2,
};
fov_array.dimensions[3] = (ZarrDimensionProperties){
    .name = "x",
    .type = ZarrDimensionType_Space,
    .array_size_px = 512,
    .chunk_size_px = 256,
    .shard_size_chunks = 2,
};

// Create well with field of view
ZarrHCSWell well = {
    .row_name = "A",
    .column_name = "1",
};

ZarrHCSWell_create_image_array(&well, 1);
well.images[0] = (ZarrHCSFieldOfView){
    .path = "fov1",
    .acquisition_id = 0,
    .has_acquisition_id = true,
    .array_settings = &fov_array,
};

// Create plate
ZarrHCSPlate plate = {
    .path = "experiment_plate",
    .name = "My HCS Experiment",
};

// Set up row and column names
ZarrHCSPlate_create_row_name_array(&plate, 8);
const char* row_names[] = {"A", "B", "C", "D", "E", "F", "G", "H"};
for (int i = 0; i < 8; i++) {
    plate.row_names[i] = row_names[i];
}

ZarrHCSPlate_create_column_name_array(&plate, 12);
const char* col_names[] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
for (int i = 0; i < 12; i++) {
    plate.column_names[i] = col_names[i];
}

// Add wells and acquisitions
ZarrHCSPlate_create_well_array(&plate, 1);
plate.wells[0] = well;

ZarrHCSPlate_create_acquisition_array(&plate, 1);
plate.acquisitions[0] = (ZarrHCSAcquisition){
    .id = 0,
    .name = "Measurement_01",
    .start_time = 1343731272000,
    .has_start_time = true,
    .end_time = 1343737645000,
    .has_end_time = true,
};

// Create HCS settings
ZarrHCSSettings hcs_settings = {
    .plates = &plate,
    .plate_count = 1,
};

// Configure stream
ZarrStreamSettings settings = {
    .store_path = "hcs_experiment.zarr",
    .version = ZarrVersion_3,
    .overwrite = true,
    .arrays = NULL,
    .array_count = 0,
    .hcs_settings = &hcs_settings,
};

ZarrStream* stream = ZarrStream_create(&settings);

// Write data
uint16_t* frame_data = /* your image data */;
size_t frame_size = 3 * 512 * 512 * sizeof(uint16_t);
size_t bytes_written;

ZarrStream_append(stream, frame_data, frame_size, &bytes_written, "experiment_plate/A/1/fov1");

// Cleanup
ZarrStream_destroy(stream);
ZarrHCSPlate_destroy_well_array(&plate);
ZarrArraySettings_destroy_dimension_array(&fov_array);
```

The resulting dataset will include proper OME-NGFF metadata for [plates](https://ngff.openmicroscopy.org/latest/#plate-md) and [wells](https://ngff.openmicroscopy.org/latest/#well-md).

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
