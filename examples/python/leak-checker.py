# Zarr V3 with LZ4 compression to filesystem
import numpy as np
from acquire_zarr import (
    StreamSettings, ZarrStream, Dimension, DimensionType, ZarrVersion,
    DataType, Compressor, CompressionCodec, CompressionSettings
)

frame_width = 14192
frame_height = 10640
chunk_width = chunk_height = 128

shard_height = (frame_height + chunk_height + 1) // chunk_height
shard_width = (frame_width + chunk_width + 1) // chunk_width

chunk_planes = 32


def make_sample_data():
    return np.random.randint(
        0, 65535,
        (1, frame_height, frame_width),
        dtype=np.uint16
    )

def main():
    settings = StreamSettings()

    # Configure compression
    settings.compression = CompressionSettings(
        compressor=Compressor.BLOSC1,
        codec=CompressionCodec.BLOSC_LZ4,
        level=1,
        shuffle=1,
    )

    # Configure 5D array (t, c, z, y, x)
    settings.dimensions.extend([
        Dimension(
            name="t",
            kind=DimensionType.TIME,
            array_size_px=0,
            chunk_size_px=chunk_planes,
            shard_size_chunks=1,
        ),
        Dimension(
            name="y",
            kind=DimensionType.SPACE,
            array_size_px=frame_height,
            chunk_size_px=chunk_height,
            shard_size_chunks=shard_height,
        ),
        Dimension(
            name="x",
            kind=DimensionType.SPACE,
            array_size_px=frame_width,
            chunk_size_px=chunk_width,
            shard_size_chunks=shard_width,
        ),
    ])

    settings.store_path = "leak-checker.zarr"
    settings.version = ZarrVersion.V3
    settings.data_type = DataType.UINT16
    settings.multiscale = True

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data
    frame = make_sample_data()
    for frame_id in range(1000):
        print("Appending frame", frame_id + 1)
        stream.append(frame)


if __name__ == "__main__":
    main()
