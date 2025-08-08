# Stream multiple arrays to filesystem
import numpy as np

from acquire_zarr import (
    ArraySettings, StreamSettings, ZarrStream, Dimension, DimensionType,
    DownsamplingMethod, Compressor, CompressionCodec, CompressionSettings
)

from typing import Tuple


def make_sample_data(shape: Tuple[int, ...], dtype: np.dtype) -> np.ndarray:
    is_int = np.issubdtype(dtype, np.integer)
    typemin = np.iinfo(dtype).min if is_int else np.finfo(dtype).min
    typemax = np.iinfo(dtype).max if is_int else np.finfo(dtype).max

    if is_int:
        return np.random.randint(
            typemin, typemax,
            shape,
            dtype=dtype
        )
    elif np.issubdtype(dtype, np.floating):
        return np.random.uniform(
            typemin, typemax,
            shape
        ).astype(dtype)
    else:
        raise ValueError(f"Unsupported data type: {dtype}")


def main():
    settings = StreamSettings(
        arrays=[
            ArraySettings(
                output_key="path/to/uint16_array",
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
                        array_size_px=0,
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
            ),
            ArraySettings(
                output_key="a/float32/array",
                compression=CompressionSettings(
                    compressor=Compressor.BLOSC1,
                    codec=CompressionCodec.BLOSC_ZSTD,
                    level=3,
                    shuffle=2,
                ),
                dimensions=[
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
                data_type=np.float32,
                downsampling_method=DownsamplingMethod.MEAN
            ),
            ArraySettings(
                output_key="labels",
                dimensions=[
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
                data_type=np.uint8,
                downsampling_method=DownsamplingMethod.MAX
            )
        ],
        store_path="output_multiarray.zarr",
        overwrite=True
    )

    # Create stream
    stream = ZarrStream(settings)

    # Write sample data to each array
    stream.append(make_sample_data((10, 8, 6, 48, 64), np.uint16), "path/to/uint16_array")
    stream.append(make_sample_data((6, 48, 64), np.float32), "a/float32/array")
    stream.append(make_sample_data((6, 48, 64), np.uint8), "labels")


if __name__ == "__main__":
    main()
