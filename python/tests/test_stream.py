#!/usr/bin/env python3
from pickle import FALSE

import dotenv
import json
from pathlib import Path
import os
import shutil
from typing import Optional

import numpy as np
import pytest
import zarr
from numcodecs import blosc as ncblosc
from zarr.codecs import blosc as zblosc
import s3fs
from skimage.transform import downscale_local_mean

dotenv.load_dotenv()

from acquire_zarr import (
    StreamSettings,
    ZarrStream,
    Compressor,
    CompressionCodec,
    CompressionSettings,
    S3Settings,
    Dimension,
    DimensionType,
    ZarrVersion,
    DataType,
    LogLevel,
    DownsamplingMethod,
    set_log_level,
    get_log_level,
)


@pytest.fixture(scope="function")
def settings():
    s = StreamSettings()
    s.custom_metadata = json.dumps({"foo": "bar"})
    s.dimensions.extend(
        [
            Dimension(
                name="t",
                kind=DimensionType.TIME,
                array_size_px=0,
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
        ]
    )

    return s


@pytest.fixture(scope="module")
def s3_settings():
    if (
        "ZARR_S3_ENDPOINT" not in os.environ
        or "ZARR_S3_BUCKET_NAME" not in os.environ
        or "AWS_ACCESS_KEY_ID" not in os.environ
        or "AWS_SECRET_ACCESS_KEY" not in os.environ
    ):
        yield None
    else:
        settings = S3Settings(
            endpoint=os.environ["ZARR_S3_ENDPOINT"],
            bucket_name=os.environ["ZARR_S3_BUCKET_NAME"],
        )
        if "ZARR_S3_REGION" in os.environ:
            settings.region = os.environ["ZARR_S3_REGION"]

        yield settings


@pytest.fixture(scope="function")
def store_path(tmp_path):
    yield tmp_path
    shutil.rmtree(tmp_path)


def validate_v2_metadata(store_path: Path):
    assert (store_path / ".zattrs").is_file()
    with open(store_path / ".zattrs", "r") as fh:
        data = json.load(fh)
        axes = data["multiscales"][0]["axes"]
        assert axes[0]["name"] == "t"
        assert axes[0]["type"] == "time"

        assert axes[1]["name"] == "y"
        assert axes[1]["type"] == "space"

        assert axes[2]["name"] == "x"
        assert axes[2]["type"] == "space"

    assert (store_path / ".zgroup").is_file()
    with open(store_path / ".zgroup", "r") as fh:
        data = json.load(fh)
        assert data["zarr_format"] == 2

    assert not (store_path / "acquire.json").is_file()


def validate_v3_metadata(store_path: Path):
    assert (store_path / "zarr.json").is_file()
    with open(store_path / "zarr.json", "r") as fh:
        data = json.load(fh)
        assert data["zarr_format"] == 3
        assert data["node_type"] == "group"
        assert data["consolidated_metadata"] is None

        axes = data["attributes"]["ome"]["multiscales"][0]["axes"]
        assert axes[0]["name"] == "t"
        assert axes[0]["type"] == "time"

        assert axes[1]["name"] == "y"
        assert axes[1]["type"] == "space"

        assert axes[2]["name"] == "x"
        assert axes[2]["type"] == "space"

    assert not (store_path / "acquire.json").is_file()


@pytest.mark.parametrize(
    ("version",),
    [
        (ZarrVersion.V2,),
        (ZarrVersion.V3,),
    ],
)
def test_create_stream(
    settings: StreamSettings,
    store_path: Path,
    request: pytest.FixtureRequest,
    version: ZarrVersion,
):
    settings.store_path = str(store_path / f"{request.node.name}.zarr")
    settings.version = version
    stream = ZarrStream(settings)
    assert stream

    store_path = Path(settings.store_path)

    del stream  # close the stream, flush the files

    # check that the stream created the zarr store
    assert store_path.is_dir()

    if version == ZarrVersion.V2:
        validate_v2_metadata(store_path)

        # no data written, so no array metadata
        assert not (store_path / "0" / ".zarray").exists()
    else:
        validate_v3_metadata(store_path)

        # no data written, so no array metadata
        assert not (store_path / "meta" / "0.array.json").exists()


@pytest.mark.parametrize(
    (
        "version",
        "compression_codec",
    ),
    [
        (
            ZarrVersion.V2,
            None,
        ),
        (
            ZarrVersion.V2,
            CompressionCodec.BLOSC_LZ4,
        ),
        (
            ZarrVersion.V2,
            CompressionCodec.BLOSC_ZSTD,
        ),
        (
            ZarrVersion.V3,
            None,
        ),
        (
            ZarrVersion.V3,
            CompressionCodec.BLOSC_LZ4,
        ),
        (
            ZarrVersion.V3,
            CompressionCodec.BLOSC_ZSTD,
        ),
    ],
)
def test_stream_data_to_filesystem(
    settings: StreamSettings,
    store_path: Path,
    version: ZarrVersion,
    compression_codec: Optional[CompressionCodec],
):
    settings.store_path = str(store_path / "test.zarr")
    settings.version = version
    if compression_codec is not None:
        settings.compression = CompressionSettings(
            compressor=Compressor.BLOSC1,
            codec=compression_codec,
            level=1,
            shuffle=1,
        )
    settings.data_type = DataType.UINT16

    stream = ZarrStream(settings)
    assert stream

    data = np.zeros(
        (
            2 * settings.dimensions[0].chunk_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.uint16,
    )
    for i in range(data.shape[0]):
        data[i, :, :] = i

    stream.append(data)

    del stream  # close the stream, flush the files

    chunk_size_bytes = data.dtype.itemsize
    for dim in settings.dimensions:
        chunk_size_bytes *= dim.chunk_size_px

    shard_size_bytes = chunk_size_bytes
    table_size_bytes = 16  # 2 * sizeof(uint64_t)
    if version == ZarrVersion.V3:
        for dim in settings.dimensions:
            shard_size_bytes *= dim.shard_size_chunks
            table_size_bytes *= dim.shard_size_chunks
    shard_size_bytes = (
        shard_size_bytes + table_size_bytes + 4
    )  # 4 bytes for crc32c checksum

    group = zarr.open(settings.store_path, mode="r")
    array = group["0"]

    assert array.shape == data.shape
    for i in range(array.shape[0]):
        assert np.array_equal(array[i, :, :], data[i, :, :])

    metadata = array.metadata
    if compression_codec is not None:
        if version == ZarrVersion.V2:
            cname = (
                "lz4"
                if compression_codec == CompressionCodec.BLOSC_LZ4
                else "zstd"
            )
            compressor = metadata.compressor
            assert compressor.cname == cname
            assert compressor.clevel == 1
            assert compressor.shuffle == ncblosc.SHUFFLE

            # check that the data is compressed
            assert (store_path / "test.zarr" / "0" / "0" / "0" / "0").is_file()
            assert (
                store_path / "test.zarr" / "0" / "0" / "0" / "0"
            ).stat().st_size <= chunk_size_bytes
        else:
            cname = (
                zblosc.BloscCname.lz4
                if compression_codec == CompressionCodec.BLOSC_LZ4
                else zblosc.BloscCname.zstd
            )
            blosc_codec = metadata.codecs[0].codecs[1]
            assert blosc_codec.cname == cname
            assert blosc_codec.clevel == 1
            assert blosc_codec.shuffle == zblosc.BloscShuffle.shuffle

            assert (
                store_path / "test.zarr" / "0" / "c" / "0" / "0" / "0"
            ).is_file()
            assert (
                store_path / "test.zarr" / "0" / "c" / "0" / "0" / "0"
            ).stat().st_size <= shard_size_bytes
    else:
        if version == ZarrVersion.V2:
            assert metadata.compressor is None

            assert (store_path / "test.zarr" / "0" / "0" / "0" / "0").is_file()
            assert (
                store_path / "test.zarr" / "0" / "0" / "0" / "0"
            ).stat().st_size == chunk_size_bytes
        else:
            assert len(metadata.codecs[0].codecs) == 1

            assert (
                store_path / "test.zarr" / "0" / "c" / "0" / "0" / "0"
            ).is_file()
            assert (
                store_path / "test.zarr" / "0" / "c" / "0" / "0" / "0"
            ).stat().st_size == shard_size_bytes


@pytest.mark.parametrize(
    (
        "version",
        "compression_codec",
    ),
    [
        (
            ZarrVersion.V2,
            None,
        ),
        (
            ZarrVersion.V2,
            CompressionCodec.BLOSC_LZ4,
        ),
        (
            ZarrVersion.V2,
            CompressionCodec.BLOSC_ZSTD,
        ),
        (
            ZarrVersion.V3,
            None,
        ),
        (
            ZarrVersion.V3,
            CompressionCodec.BLOSC_LZ4,
        ),
        (
            ZarrVersion.V3,
            CompressionCodec.BLOSC_ZSTD,
        ),
    ],
)
def test_stream_data_to_s3(
    settings: StreamSettings,
    s3_settings: Optional[S3Settings],
    request: pytest.FixtureRequest,
    version: ZarrVersion,
    compression_codec: Optional[CompressionCodec],
):
    if s3_settings is None:
        pytest.skip("S3 settings not set")

    settings.store_path = f"{request.node.name}.zarr".replace("[", "").replace(
        "]", ""
    )
    settings.version = version
    settings.s3 = s3_settings
    settings.data_type = DataType.UINT16
    if compression_codec is not None:
        settings.compression = CompressionSettings(
            compressor=Compressor.BLOSC1,
            codec=compression_codec,
            level=1,
            shuffle=1,
        )

    stream = ZarrStream(settings)
    assert stream

    data = np.random.randint(
        0,
        65535,
        (
            2 * settings.dimensions[0].chunk_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.uint16,
    )
    stream.append(data)

    del stream  # close the stream, flush the data

    store = zarr.storage.FsspecStore.from_url(
        f"s3://{settings.s3.bucket_name}/{settings.store_path}",
        storage_options={
            "key": os.environ.get("AWS_ACCESS_KEY_ID"),
            "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "client_kwargs": {"endpoint_url": s3_settings.endpoint},
        },
    )
    group = zarr.group(store=store)
    array = group["0"]

    assert array.shape == data.shape
    for i in range(array.shape[0]):
        assert np.array_equal(array[i, :, :], data[i, :, :])

    metadata = array.metadata
    if compression_codec is not None:
        if version == ZarrVersion.V2:
            cname = (
                "lz4"
                if compression_codec == CompressionCodec.BLOSC_LZ4
                else "zstd"
            )
            compressor = metadata.compressor
            assert compressor.cname == cname
            assert compressor.clevel == 1
            assert compressor.shuffle == ncblosc.SHUFFLE
        else:
            cname = (
                zblosc.BloscCname.lz4
                if compression_codec == CompressionCodec.BLOSC_LZ4
                else zblosc.BloscCname.zstd
            )
            blosc_codec = metadata.codecs[0].codecs[1]
            assert blosc_codec.cname == cname
            assert blosc_codec.clevel == 1
            assert blosc_codec.shuffle == zblosc.BloscShuffle.shuffle
    else:
        if version == ZarrVersion.V2:
            assert metadata.compressor is None
        else:
            assert len(metadata.codecs[0].codecs) == 1

    # cleanup
    s3 = s3fs.S3FileSystem(
        key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"endpoint_url": settings.s3.endpoint},
    )
    s3.rm(f"{settings.s3.bucket_name}/{settings.store_path}", recursive=True)


@pytest.mark.parametrize(
    ("level",),
    [
        (LogLevel.DEBUG,),
        (LogLevel.INFO,),
        (LogLevel.WARNING,),
        (LogLevel.ERROR,),
        (LogLevel.NONE,),
    ],
)
def test_set_log_level(level: LogLevel):
    set_log_level(level)
    assert get_log_level() == level


@pytest.mark.parametrize(
    ("version", "overwrite"),
    [
        (ZarrVersion.V2, False),
        (ZarrVersion.V2, True),
        (ZarrVersion.V3, False),
        (ZarrVersion.V3, True),
    ],
)
def test_write_custom_metadata(
    settings: StreamSettings,
    store_path: Path,
    request: pytest.FixtureRequest,
    version: ZarrVersion,
    overwrite: bool,
):
    settings.store_path = str(store_path / f"{request.node.name}.zarr")
    settings.version = version
    stream = ZarrStream(settings)
    assert stream

    metadata = json.dumps({"foo": "bar"})
    assert stream.write_custom_metadata(metadata, True)

    # don't allow overwriting the metadata
    metadata = json.dumps({"baz": "qux"})
    overwrite_result = stream.write_custom_metadata(
        metadata, overwrite=overwrite
    )
    assert overwrite_result == overwrite

    del stream

    assert (Path(settings.store_path) / "acquire.json").is_file()
    with open(Path(settings.store_path) / "acquire.json", "r") as fh:
        data = json.load(fh)

    if overwrite:  # the metadata is overwritten
        assert data["baz"] == "qux"
        assert "foo" not in data
    else:  # the originally written metadata is preserved
        assert data["foo"] == "bar"
        assert "baz" not in data


def test_write_transposed_array(
        store_path: Path,
):
    settings = StreamSettings(
        dimensions=[
            Dimension(
                name="t",
                kind=DimensionType.TIME,
                array_size_px=2,
                chunk_size_px=2,
                shard_size_chunks=1,
            ),
            Dimension(
                name="c",
                kind=DimensionType.CHANNEL,
                array_size_px=1,
                chunk_size_px=1,
                shard_size_chunks=1,
            ),
            Dimension(
                name="z",
                kind=DimensionType.SPACE,
                array_size_px=40,
                chunk_size_px=20,
                shard_size_chunks=1,
            ),
            Dimension(
                name="y",
                kind=DimensionType.SPACE,
                array_size_px=30,
                chunk_size_px=15,
                shard_size_chunks=1,
            ),
            Dimension(
                name="x",
                kind=DimensionType.SPACE,
                array_size_px=20,
                chunk_size_px=10,
                shard_size_chunks=1,
            ),
        ]
    )
    settings.store_path = str(store_path / "test.zarr")
    settings.version = ZarrVersion.V3
    settings.data_type = DataType.INT32

    data = np.random.randint(
        -2**16,
        2**16 - 1,
        (
            settings.dimensions[0].chunk_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
            settings.dimensions[4].array_size_px,
            settings.dimensions[3].array_size_px,
        ),
        dtype=np.int32,
        )
    data = np.transpose(data, (0, 1, 2, 4, 3))

    stream = ZarrStream(settings)
    assert stream

    stream.append(data)

    del stream  # close the stream, flush the files

    group = zarr.open(settings.store_path, mode="r")
    array = group["0"]

    assert data.shape == array.shape
    np.testing.assert_array_equal(data, array)


def test_column_ragged_sharding(
        store_path: Path,
):
    settings = StreamSettings(
        dimensions=[
            Dimension(
                name="z",
                kind=DimensionType.SPACE,
                array_size_px=2,
                chunk_size_px=1,
                shard_size_chunks=2,
            ),
            Dimension(
                name="y",
                kind=DimensionType.SPACE,
                array_size_px=1080,
                chunk_size_px=64,
                shard_size_chunks=2,
            ),
            Dimension(
                name="x",
                kind=DimensionType.SPACE,
                array_size_px=1080,
                chunk_size_px=64,
                shard_size_chunks=2,
            ),
        ]
    )
    settings.store_path = str(store_path / "test.zarr")
    settings.version = ZarrVersion.V3
    settings.data_type = DataType.INT32

    data = np.random.randint(
        -2 ** 16,
        2 ** 16 - 1,
        (
            settings.dimensions[0].array_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.int32,
    )

    stream = ZarrStream(settings)
    assert stream

    stream.append(data)

    del stream  # close the stream, flush the files

    array = zarr.open(settings.store_path, mode="r")["0"]

    assert data.shape == array.shape
    np.testing.assert_array_equal(data, array)


def test_custom_dimension_units_and_scales(store_path: Path):
    settings = StreamSettings(
        dimensions=[
            Dimension(
                name="z",
                kind=DimensionType.SPACE,
                unit="micron",
                scale=0.1,
                array_size_px=2,
                chunk_size_px=1,
                shard_size_chunks=2,
            ),
            Dimension(
                name="y",
                kind=DimensionType.SPACE,
                unit="micrometer",
                scale=0.9,
                array_size_px=1080,
                chunk_size_px=64,
                shard_size_chunks=2,
            ),
            Dimension(
                name="x",
                kind=DimensionType.SPACE,
                unit="nanometer",
                scale=1.1,
                array_size_px=1080,
                chunk_size_px=64,
                shard_size_chunks=2,
            ),
        ]
    )
    settings.store_path = str(store_path / "test.zarr")
    settings.version = ZarrVersion.V3
    settings.data_type = DataType.INT32

    data = np.random.randint(
        -2 ** 16,
        2 ** 16 - 1,
        (
            settings.dimensions[0].array_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.int32,
    )

    stream = ZarrStream(settings)
    assert stream

    stream.append(data)

    del stream  # close the stream, flush the files

    group = zarr.open(settings.store_path, mode="r")
    array = group["0"]

    assert data.shape == array.shape
    np.testing.assert_array_equal(data, array)

    # Check custom units and scales
    multiscale = group.attrs["ome"]["multiscales"][0]
    axes = multiscale["axes"]
    assert len(axes) == 3
    z, y, x = axes

    assert z["name"] == "z"
    assert z["type"] == "space"
    assert z["unit"] == "micron"

    assert y["name"] == "y"
    assert y["type"] == "space"
    assert y["unit"] == "micrometer"

    assert x["name"] == "x"
    assert x["type"] == "space"
    assert x["unit"] == "nanometer"

    z_scale, y_scale, x_scale = multiscale["datasets"][0]["coordinateTransformations"][0]["scale"]

    assert z_scale == 0.1
    assert y_scale == 0.9
    assert x_scale == 1.1


@pytest.mark.parametrize(("method",),
                         [(DownsamplingMethod.MEAN,), (DownsamplingMethod.MIN,), (DownsamplingMethod.MAX,)])
def test_2d_multiscale_stream(store_path: Path, method: DownsamplingMethod):
    settings = StreamSettings(
        dimensions=[
            Dimension(
                name="t",
                kind=DimensionType.TIME,
                array_size_px=50,
                chunk_size_px=50,
                shard_size_chunks=1,
            ),
            Dimension(
                name="y",
                kind=DimensionType.SPACE,
                array_size_px=48,
                chunk_size_px=24,
                shard_size_chunks=1,
            ),
            Dimension(
                name="x",
                kind=DimensionType.SPACE,
                array_size_px=64,
                chunk_size_px=32,
                shard_size_chunks=1,
            ),
        ]
    )
    settings.store_path = str(store_path / "test.zarr")
    settings.version = ZarrVersion.V3
    settings.data_type = DataType.INT32
    settings.multiscale = True
    settings.downsampling_method = method

    data = np.random.randint(
        -2 ** 16,
        2 ** 16 - 1,
        (
            settings.dimensions[0].array_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.int32,
    )

    stream = ZarrStream(settings)
    assert stream

    stream.append(data)

    del stream

    group = zarr.open(settings.store_path, mode="r")
    assert "0" in group

    full_res = group["0"]
    assert data.shape == full_res.shape
    np.testing.assert_array_equal(data, full_res)

    assert "1" in group
    downsampled = group["1"]
    assert downsampled.shape == (50, 24, 32)

    expected = np.zeros((downsampled.shape[1], downsampled.shape[2]), dtype=data.dtype)
    if method == DownsamplingMethod.MEAN:
        for plane in range(downsampled.shape[0]):
            expected = downscale_local_mean(data[plane, :, :], (2, 2)).astype(data.dtype)
            actual = downsampled[plane, :, :]

            np.testing.assert_array_equal(actual, expected)
    elif method == DownsamplingMethod.MIN:
        for plane in range(downsampled.shape[0]):
            actual = downsampled[plane, :, :]

            for row in range(0, data.shape[1], 2):
                for col in range(0, data.shape[2], 2):
                    expected[row // 2, col // 2] = np.min(
                        data[plane, row:row + 2, col:col + 2]
                    )

            np.testing.assert_array_equal(actual, expected)
    elif method == DownsamplingMethod.MAX:
        for plane in range(downsampled.shape[0]):
            for row in range(0, data.shape[1], 2):
                for col in range(0, data.shape[2], 2):
                    expected[row // 2, col // 2] = np.max(
                        data[plane, row:row + 2, col:col + 2]
                    )

            np.testing.assert_array_equal(downsampled[plane, :, :], expected)


@pytest.mark.parametrize(("method",),
                         [(DownsamplingMethod.MEAN,), (DownsamplingMethod.MIN,), (DownsamplingMethod.MAX,)])
def test_3d_multiscale_stream(store_path: Path, method: DownsamplingMethod):
    settings = StreamSettings(
        dimensions=[
            Dimension(
                name="z",
                kind=DimensionType.SPACE,
                array_size_px=100,
                chunk_size_px=50,
                shard_size_chunks=1,
            ),
            Dimension(
                name="y",
                kind=DimensionType.SPACE,
                array_size_px=48,
                chunk_size_px=24,
                shard_size_chunks=1,
            ),
            Dimension(
                name="x",
                kind=DimensionType.SPACE,
                array_size_px=64,
                chunk_size_px=32,
                shard_size_chunks=1,
            ),
        ]
    )
    settings.store_path = str(store_path / "test.zarr")
    settings.version = ZarrVersion.V3
    settings.data_type = DataType.UINT16
    settings.multiscale = True
    settings.downsampling_method = method

    data = np.random.randint(
        0,
        2 ** 16 - 1,
        (
            settings.dimensions[0].array_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.uint16,
    )

    stream = ZarrStream(settings)
    assert stream

    stream.append(data)

    del stream

    group = zarr.open(settings.store_path, mode="r")
    assert "0" in group

    full_res = group["0"]
    assert data.shape == full_res.shape
    np.testing.assert_array_equal(data, full_res)

    assert "1" in group
    downsampled = group["1"]
    assert downsampled.shape == (50, 24, 32)

    expected = np.zeros((downsampled.shape[1], downsampled.shape[2]), dtype=data.dtype)
    if method == DownsamplingMethod.MEAN:
        for plane in range(downsampled.shape[0]):
            actual = downsampled[plane, :, :]

            expected = downscale_local_mean(data[(2 * plane): 2 * (plane + 1), :, :], (2, 2, 2)).astype(
                data.dtype).squeeze()

            np.testing.assert_allclose(actual, expected, atol=1)  # we may round slightly differently than skimage
    elif method == DownsamplingMethod.MIN:
        for plane in range(downsampled.shape[0]):
            actual = downsampled[plane, :, :]

            for row in range(0, data.shape[1], 2):
                for col in range(0, data.shape[2], 2):
                    expected[row // 2, col // 2] = np.min(
                        data[(2 * plane):2 * (plane + 1), row:row + 2, col:col + 2]
                    )

            np.testing.assert_array_equal(actual, expected)
    elif method == DownsamplingMethod.MAX:
        for plane in range(downsampled.shape[0]):
            actual = downsampled[plane, :, :]

            for row in range(0, data.shape[1], 2):
                for col in range(0, data.shape[2], 2):
                    expected[row // 2, col // 2] = np.max(
                        data[(2 * plane):2 * (plane + 1), row:row + 2, col:col + 2]
                    )

            np.testing.assert_array_equal(actual, expected)


@pytest.mark.parametrize(("output_key", "multiscale"),
                         [
                             ("labels", False),
                             ("path/to/data", False),
                             ("a/nested/multiscale/group", True),
                         ])
def test_stream_data_to_named_array(
        settings: StreamSettings,
        store_path: Path,
        request: pytest.FixtureRequest,
        output_key: str,
        multiscale: bool,
):
    settings.store_path = str(store_path / f"stream_to_named_array_{output_key.replace('/', '_')}.zarr")
    settings.version = ZarrVersion.V3
    settings.output_key = output_key
    settings.multiscale = multiscale
    settings.data_type = DataType.UINT16

    stream = ZarrStream(settings)
    assert stream

    # Create test data
    data = np.zeros(
        (
            2 * settings.dimensions[0].chunk_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.uint16,
    )
    # Fill with recognizable pattern
    for i in range(data.shape[0]):
        data[i, :, :] = i + 1

    stream.append(data)

    del stream  # close the stream, flush the files

    # Now verify the data is stored in the expected location
    store_path_obj = Path(settings.store_path)
    assert store_path_obj.is_dir()

    # Open the Zarr group and navigate to the correct path
    root_group = zarr.open(settings.store_path, mode="r")

    # Navigate through the path to get to the array
    current_group = root_group
    path_parts = output_key.split('/')

    for part in path_parts:
        assert part in current_group, f"Path part '{part}' not found in group"
        current_group = current_group[part]

    array = current_group["0"] if multiscale else current_group

    # Verify array shape and contents
    assert array.shape == data.shape
    assert np.array_equal(array, data)
