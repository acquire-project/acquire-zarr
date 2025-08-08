# Stream to S3
import numpy as np

# Ensure that you have set your S3 credentials in the environment variables
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and optionally AWS_SESSION_TOKEN
# BEFORE importing acquire_zarr
from acquire_zarr import (
    ArraySettings, StreamSettings, ZarrStream, Dimension, DimensionType,
    DataType, S3Settings
)


def make_sample_data():
    return np.random.randint(
        0, 65535,
        (5, 2, 48, 64),  # Shape matches chunk sizes
        dtype=np.uint16
    )


def main():
    settings = StreamSettings()

    # Configure S3
    settings.s3 = S3Settings(
        endpoint="http://localhost:9000",
        bucket_name="my-bucket",
        region="us-east-2"
    )

    # Configure 4D array (t, z, y, x)
    settings.arrays = [
        ArraySettings(
            dimensions=[
                Dimension(
                    name="t",
                    kind=DimensionType.TIME,
                    array_size_px=0,  # Unlimited
                    chunk_size_px=5,
                    shard_size_chunks=2,
                ),
                Dimension(
                    name="z",
                    kind=DimensionType.SPACE,
                    array_size_px=10,
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
            data_type= DataType.UINT16,
        )
    ]

    settings.store_path = "output_s3.zarr"

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data
    stream.append(make_sample_data())


if __name__ == "__main__":
    main()
