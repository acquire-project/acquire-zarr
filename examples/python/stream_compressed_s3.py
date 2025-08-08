# Stream to S3 with Zstd compression
import numpy as np

# Ensure that you have set your S3 credentials in the environment variables
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and optionally AWS_SESSION_TOKEN
# BEFORE importing acquire_zarr
from acquire_zarr import (
    ArraySettings, StreamSettings, ZarrStream, Dimension, DimensionType,
    DataType, S3Settings, Compressor, CompressionCodec, CompressionSettings
)


def make_sample_data():
    """Generate sample data with moving diagonal pattern"""
    width, height = 64, 48
    frames = []
    
    for t in range(50):
        frame = np.zeros((height, width), dtype=np.uint16)
        
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
                radius = int(np.sqrt(dx*dx + dy*dy))
                
                # Modulate with concentric circles
                if radius % 16 < 8:
                    intensity = int(intensity * 0.7)
                
                frame[y, x] = intensity
        
        frames.append(frame)
    
    return np.array(frames)


def main():
    settings = StreamSettings()

    # Configure S3
    settings.s3 = S3Settings(
        endpoint="http://localhost:9000",
        bucket_name="my-bucket"
    )

    # Configure 3D array (t, y, x) with Zstd compression
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

    settings.store_path = "output_v3_compressed_s3.zarr"

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data
    sample_data = make_sample_data()
    stream.append(sample_data)


if __name__ == "__main__":
    main()
