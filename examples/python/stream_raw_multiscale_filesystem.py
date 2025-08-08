# Stream to filesystem with multiscale, no compression
import numpy as np
from acquire_zarr import (
    ArraySettings, StreamSettings, ZarrStream, Dimension, DimensionType,
    DownsamplingMethod
)


def make_sample_data():
    """Generate sample data matching the 5D structure (t, c, z, y, x)"""
    # Shape: (10 timepoints, 8 channels, 6 z-slices, 48 height, 64 width)
    return np.random.randint(
        0, 65535,
        (10, 8, 6, 48, 64),
        dtype=np.uint16
    )


def main():
    settings = StreamSettings()

    # Configure 5D array (t, c, z, y, x) with multiscale, no compression
    settings.arrays = [
        ArraySettings(
            dimensions=[
                Dimension(
                    name="t",
                    kind=DimensionType.TIME,
                    array_size_px=10,
                    chunk_size_px=5,
                    shard_size_chunks=2,
                ),
                Dimension(
                    name="c",
                    kind=DimensionType.CHANNEL,
                    array_size_px=8,
                    chunk_size_px=4,
                    shard_size_chunks=2,
                ),
                Dimension(
                    name="z",
                    kind=DimensionType.SPACE,
                    array_size_px=6,
                    chunk_size_px=2,
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
                    chunk_size_px=16,
                    shard_size_chunks=2,
                ),
            ],
            data_type=np.uint16,
            downsampling_method=DownsamplingMethod.MEAN
        )
    ]

    settings.store_path = "output_v3_multiscale.zarr"

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data
    stream.append(make_sample_data())


if __name__ == "__main__":
    main()
