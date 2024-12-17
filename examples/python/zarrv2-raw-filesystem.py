# Basic Zarr V2 to filesystem
import numpy as np
from acquire_zarr import (
    StreamSettings, ZarrStream, Dimension, DimensionType, ZarrVersion, DataType
)


def make_sample_data():
    return np.random.randint(
        0, 65535,
        (32, 48, 64),  # Shape matches chunk size for time dimension
        dtype=np.int32
    )

def main():
    # Configure stream settings
    settings = StreamSettings()

    # Configure dimensions (t, y, x)
    settings.dimensions.extend([
        Dimension(
            name="t",
            kind=DimensionType.TIME,
            array_size_px=0,  # Unlimited
            chunk_size_px=32,
            shard_size_chunks=1,
        ),
        Dimension(
            name="y",
            kind=DimensionType.SPACE,
            array_size_px=48,
            chunk_size_px=16,
            shard_size_chunks=1,
        ),
        Dimension(
            name="x",
            kind=DimensionType.SPACE,
            array_size_px=64,
            chunk_size_px=32,
            shard_size_chunks=1,
        ),
    ])

    settings.store_path = "output_v2.zarr"
    settings.version = ZarrVersion.V2
    settings.data_type = DataType.INT32

    # Create stream
    stream = ZarrStream(settings)

    # Create and write sample data
    for i in range(10):
        stream.append(make_sample_data())


if __name__ == "__main__":
    main()
