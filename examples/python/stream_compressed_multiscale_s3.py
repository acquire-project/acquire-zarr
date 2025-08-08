# Stream to S3 with multiscale and Zstd compression
import numpy as np

# Ensure that you have set your S3 credentials in the environment variables
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and optionally AWS_SESSION_TOKEN
# BEFORE importing acquire_zarr
from acquire_zarr import (
    ArraySettings, StreamSettings, ZarrStream, Dimension, DimensionType,
    DownsamplingMethod, S3Settings, Compressor, CompressionCodec, CompressionSettings
)


def make_sample_data():
    """Generate sample data with moving diagonal pattern for 4D array (t, z, y, x)"""
    width, height, depth = 64, 48, 10
    frames = []

    for t in range(10):
        volume = np.zeros((depth, height, width), dtype=np.uint16)

        for z in range(depth):
            for y in range(height):
                for x in range(width):
                    # Create a diagonal pattern that moves with time
                    diagonal = (x + y + t * 8) % 32

                    # Create intensity variation
                    if diagonal < 16:
                        intensity = diagonal * 4096  # Ramp up
                    else:
                        intensity = (31 - diagonal) * 4096  # Ramp down

                    # Add circular features
                    center_x, center_y = width // 2, height // 2
                    dx, dy = x - center_x, y - center_y
                    radius = int(np.sqrt(dx * dx + dy * dy))

                    # Modulate with concentric circles
                    if radius % 16 < 8:
                        intensity = int(intensity * 0.7)

                    volume[z, y, x] = intensity

        frames.append(volume)

    return np.array(frames)


def main():
    settings = StreamSettings()

    # Configure S3
    settings.s3 = S3Settings(
        endpoint="http://127.0.0.1:9000",
        bucket_name="my-bucket"
    )

    # Configure 4D array (t, z, y, x) with multiscale and Zstd compression
    settings.arrays = [
        ArraySettings(
            compression=CompressionSettings(
                compressor=Compressor.BLOSC1,
                codec=CompressionCodec.BLOSC_ZSTD,
                level=1,
                shuffle=1,
            ),
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
            data_type=np.uint16,
            downsampling_method=DownsamplingMethod.MEAN,
        )
    ]

    settings.store_path = "output_v3_compressed_multiscale_s3.zarr"

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data
    stream.append(make_sample_data())


if __name__ == "__main__":
    main()
