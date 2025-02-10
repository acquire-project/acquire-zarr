#!/usr/bin/env python3

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
    dotenv.load_dotenv()
    if (
        "ZARR_S3_ENDPOINT" not in os.environ
        or "ZARR_S3_BUCKET_NAME" not in os.environ
        or "ZARR_S3_ACCESS_KEY_ID" not in os.environ
        or "ZARR_S3_SECRET_ACCESS_KEY" not in os.environ
    ):
        yield None
    else:
        region = os.environ["ZARR_S3_REGION"] if "ZARR_S3_REGION" in os.environ else None
        yield S3Settings(
            endpoint=os.environ["ZARR_S3_ENDPOINT"],
            bucket_name=os.environ["ZARR_S3_BUCKET_NAME"],
            access_key_id=os.environ["ZARR_S3_ACCESS_KEY_ID"],
            secret_access_key=os.environ["ZARR_S3_SECRET_ACCESS_KEY"],
            region=region,
        )


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
        assert axes[1]["unit"] == "micrometer"

        assert axes[2]["name"] == "x"
        assert axes[2]["type"] == "space"
        assert axes[2]["unit"] == "micrometer"

    assert (store_path / ".zgroup").is_file()
    with open(store_path / ".zgroup", "r") as fh:
        data = json.load(fh)
        assert data["zarr_format"] == 2

    assert (store_path / "acquire.json").is_file()
    with open(store_path / "acquire.json", "r") as fh:
        data = json.load(fh)
        assert data["foo"] == "bar"


def validate_v3_metadata(store_path: Path):
    assert (store_path / "zarr.json").is_file()
    with open(store_path / "zarr.json", "r") as fh:
        data = json.load(fh)
        assert data["zarr_format"] == 3
        assert data["node_type"] == "group"
        assert data["consolidated_metadata"] is None

        axes = data["attributes"]["multiscales"][0]["axes"]
        assert axes[0]["name"] == "t"
        assert axes[0]["type"] == "time"

        assert axes[1]["name"] == "y"
        assert axes[1]["type"] == "space"
        assert axes[1]["unit"] == "micrometer"

        assert axes[2]["name"] == "x"
        assert axes[2]["type"] == "space"
        assert axes[2]["unit"] == "micrometer"

    assert (store_path / "acquire.json").is_file()
    with open(store_path / "acquire.json", "r") as fh:
        data = json.load(fh)
        assert data["foo"] == "bar"


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

    stream = ZarrStream(settings)
    assert stream

    data = np.random.randint(
        0,
        255,
        (
            settings.dimensions[0].chunk_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.uint8,
    )
    stream.append(data)

    del stream  # close the stream, flush the files

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
            settings.dimensions[0].chunk_size_px,
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
            "key": s3_settings.access_key_id,
            "secret": s3_settings.secret_access_key,
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
        key=settings.s3.access_key_id,
        secret=settings.s3.secret_access_key,
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
