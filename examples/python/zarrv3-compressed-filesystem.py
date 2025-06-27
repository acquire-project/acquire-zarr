# Zarr V3 with LZ4 compression to filesystem
import numpy as np
from acquire_zarr import (
    ArraySettings, StreamSettings, ZarrStream, Dimension, DimensionType, ZarrVersion,
    DataType, Compressor, CompressionCodec, CompressionSettings
)


def make_sample_data():
    return np.random.randint(
        0, 65535,
        (5, 4, 2, 48, 64),  # Shape matches chunk sizes
        dtype=np.uint16
    )


def main():
    settings = StreamSettings()

    # Configure a 5D compressed output array
    settings.arrays = [
        ArraySettings(
            compression=CompressionSettings(
                compressor=Compressor.BLOSC1,
                codec=CompressionCodec.BLOSC_LZ4,
                level=1,
                shuffle=1,
            ),
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
            data_type=DataType.UINT16,
        )
    ]

    settings.store_path = "output_v3_compressed.zarr"
    settings.version = ZarrVersion.V3

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data
    stream.append(make_sample_data())


if __name__ == "__main__":
    main()
