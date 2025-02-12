# Zarr V2 with ZSTD compression to S3
import numpy as np
from acquire_zarr import (
    StreamSettings, ZarrStream, Dimension, DimensionType, ZarrVersion,
    DataType, Compressor, CompressionCodec, CompressionSettings, S3Settings
)


def make_sample_data():
    return np.random.randint(
        0, 65535,
        (32, 3, 48, 64),  # Shape matches chunk sizes
        dtype=np.int32
    )

def main():
    settings = StreamSettings()

    # Configure S3
    settings.s3 = S3Settings(
        endpoint="http://localhost:9000",
        bucket_name="mybucket",
        access_key_id="myaccesskey",
        secret_access_key="mysecretkey",
        region="us-east-2"
    )

    # Configure compression
    settings.compression = CompressionSettings(
        compressor=Compressor.BLOSC1,
        codec=CompressionCodec.BLOSC_ZSTD,
        level=1,
        shuffle=1,
    )

    # Configure 4D array (t, c, y, x)
    settings.dimensions.extend([
        Dimension(
            name="t",
            kind=DimensionType.TIME,
            array_size_px=0,  # Unlimited
            chunk_size_px=32,
            shard_size_chunks=1,
        ),
        Dimension(
            name="c",
            kind=DimensionType.CHANNEL,
            array_size_px=3,
            chunk_size_px=3,
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

    settings.store_path = "output_v2_s3.zarr"
    settings.version = ZarrVersion.V2
    settings.data_type = DataType.INT32

    # Create stream
    stream = ZarrStream(settings)

    # Create and write sample data
    for i in range(10):
        stream.append(make_sample_data())


if __name__ == "__main__":
    main()
