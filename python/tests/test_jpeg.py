import io
import json
from pathlib import Path

import numpy as np
import pytest
import zarr
from acquire_zarr import (
    ArraySettings,
    Dimension,
    DimensionType,
    DownsamplingMethod,
    JpegSettings,
    StreamSettings,
    ZarrStream,
)

WINDOW_MAX = 4095.0


def make_settings(store_path: Path, jpeg: JpegSettings) -> StreamSettings:
    return StreamSettings(
        store_path=str(store_path / "test.zarr"),
        arrays=[
            ArraySettings(
                dimensions=[
                    Dimension(
                        name="t",
                        kind=DimensionType.TIME,
                        array_size_px=2,
                        chunk_size_px=1,
                        shard_size_chunks=1,
                    ),
                    Dimension(
                        name="z",
                        kind=DimensionType.SPACE,
                        array_size_px=16,
                        chunk_size_px=8,
                        shard_size_chunks=1,
                    ),
                    Dimension(
                        name="y",
                        kind=DimensionType.SPACE,
                        array_size_px=128,
                        chunk_size_px=64,
                        shard_size_chunks=1,
                    ),
                    Dimension(
                        name="x",
                        kind=DimensionType.SPACE,
                        array_size_px=128,
                        chunk_size_px=64,
                        shard_size_chunks=1,
                    ),
                ],
                data_type=np.uint16,
                downsampling_method=DownsamplingMethod.MEAN,
                jpeg=jpeg,
            )
        ],
    )


def smooth_data() -> np.ndarray:
    t, z, y, x = np.meshgrid(
        np.arange(2),
        np.arange(16),
        np.arange(128),
        np.arange(128),
        indexing="ij",
    )
    v = (
        0.5
        + 0.25 * np.sin(x / 9.0 + t) * np.cos(y / 11.0)
        + 0.2 * np.sin(z / 5.0)
    )
    return np.round(v * WINDOW_MAX).astype(np.uint16)


def first_chunk_of_shard(path: Path) -> bytes:
    raw = path.read_bytes()
    index = raw[-(16 + 4) :]  # one (offset, nbytes) pair, then a crc32c
    offset, nbytes = np.frombuffer(index[:16], dtype="<u8")
    return raw[offset : offset + nbytes]


def test_jpeg_settings_defaults():
    jpeg = JpegSettings()
    assert jpeg.quality == 95
    assert jpeg.window_min == 0.0
    assert jpeg.window_max == 0.0

    jpeg = JpegSettings(quality=80, window_min=10, window_max=1000)
    assert (jpeg.quality, jpeg.window_min, jpeg.window_max) == (80, 10, 1000)
    assert "JpegSettings(quality=80" in repr(jpeg)

    with pytest.raises(ValueError):
        JpegSettings(quality=0)


def test_array_settings_jpeg_property():
    array = ArraySettings()
    assert array.jpeg is None

    array.jpeg = JpegSettings(quality=90, window_max=255)
    assert array.jpeg.quality == 90
    assert "jpeg=JpegSettings" in repr(array)

    array.jpeg = None
    assert array.jpeg is None


def test_jpeg_requires_a_window_for_uint16(store_path: Path):
    with pytest.raises(RuntimeError):
        ZarrStream(make_settings(store_path, JpegSettings(quality=90)))


def test_stream_jpeg_pyramid(store_path: Path):
    settings = make_settings(
        store_path, JpegSettings(quality=90, window_max=WINDOW_MAX)
    )
    data = smooth_data()

    stream = ZarrStream(settings)
    stream.append(data)
    stream.close()

    store = Path(settings.store_path)
    group = zarr.open_group(store, mode="r")

    # level 0 is lossless and keeps its data type
    level0 = group["0"]
    assert level0.dtype == np.uint16
    np.testing.assert_array_equal(level0[:], data)

    # level 1 declares the data type of level 0; its chain maps it to uint8
    meta = json.loads((store / "1" / "zarr.json").read_text())
    assert meta["data_type"] == "uint16"
    codecs = meta["codecs"][0]["configuration"]["codecs"]
    assert [c["name"] for c in codecs] == [
        "cast_value",
        "scale_offset",
        "cast_value",
        "reshape",
        "jpeg",
    ]
    assert codecs[0]["configuration"] == {
        "data_type": "float32",
        "out_of_range": "clamp",
    }
    assert codecs[1]["configuration"]["offset"] == 0.0
    assert codecs[1]["configuration"]["scale"] == pytest.approx(
        np.float32(255.0 / WINDOW_MAX)
    )
    assert codecs[2]["configuration"] == {
        "data_type": "uint8",
        "rounding": "nearest-even",
        "out_of_range": "clamp",
    }
    assert codecs[3]["configuration"]["shape"] == [[0, 1, 2], [3]]
    assert codecs[4]["configuration"] == {
        "quality": 90,
        "encoded_color_space": "grayscale",
    }

    # decode one chunk without a zarr JPEG plugin
    image = pytest.importorskip("PIL.Image")
    jpeg = first_chunk_of_shard(store / "1" / "c" / "0" / "0" / "0" / "0")
    decoded = np.asarray(image.open(io.BytesIO(jpeg)))
    assert decoded.shape == (8 * 64, 64)

    mean = (
        data[0].astype(np.float64).reshape(8, 2, 64, 2, 64, 2).mean((1, 3, 5))
    )
    expected = mean.reshape(8 * 64, 64) * 255.0 / WINDOW_MAX
    mse = np.mean((decoded - expected) ** 2)
    assert 10 * np.log10(255**2 / mse) > 35
