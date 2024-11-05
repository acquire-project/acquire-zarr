#!/usr/bin/env python3

import dotenv
import json
from pathlib import Path
import os
import shutil
from typing import Optional

os.environ["ZARR_V3_EXPERIMENTAL_API"] = "1"
os.environ["ZARR_V3_SHARDING"] = "1"

import numpy as np
import pytest
import zarr
from numcodecs import blosc
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
    get_log_level
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
        yield S3Settings(
            endpoint=os.environ["ZARR_S3_ENDPOINT"],
            bucket_name=os.environ["ZARR_S3_BUCKET_NAME"],
            access_key_id=os.environ["ZARR_S3_ACCESS_KEY_ID"],
            secret_access_key=os.environ["ZARR_S3_SECRET_ACCESS_KEY"],
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

    assert (store_path / "0").is_dir()


def validate_v3_metadata(store_path: Path):
    assert (store_path / "zarr.json").is_file()
    with open(store_path / "zarr.json", "r") as fh:
        data = json.load(fh)
        assert data["extensions"] == []
        assert (
            data["metadata_encoding"]
            == "https://purl.org/zarr/spec/protocol/core/3.0"
        )
        assert (
            data["zarr_format"]
            == "https://purl.org/zarr/spec/protocol/core/3.0"
        )
        assert data["metadata_key_suffix"] == ".json"

    assert (store_path / "meta").is_dir()
    assert (store_path / "meta" / "root.group.json").is_file()
    with open(store_path / "meta" / "root.group.json", "r") as fh:
        data = json.load(fh)
        axes = data["attributes"]["multiscales"][0]["axes"]
        assert axes[0]["name"] == "t"
        assert axes[0]["type"] == "time"

        assert axes[1]["name"] == "y"
        assert axes[1]["type"] == "space"
        assert axes[1]["unit"] == "micrometer"

        assert axes[2]["name"] == "x"
        assert axes[2]["type"] == "space"
        assert axes[2]["unit"] == "micrometer"

    assert (store_path / "meta" / "acquire.json").is_file()
    with open(store_path / "meta" / "acquire.json", "r") as fh:
        data = json.load(fh)
        assert data["foo"] == "bar"


def get_directory_store(version: ZarrVersion, store_path: str):
    if version == ZarrVersion.V2:
        return zarr.DirectoryStore(store_path)
    else:
        return zarr.DirectoryStoreV3(store_path)


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
    request: pytest.FixtureRequest,
    version: ZarrVersion,
    compression_codec: Optional[CompressionCodec],
):
    settings.store_path = str(store_path / f"{request.node.name}.zarr")
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

    group = zarr.open(
        store=get_directory_store(version, settings.store_path), mode="r"
    )
    data = group["0"]

    assert data.shape == (
        settings.dimensions[0].chunk_size_px,
        settings.dimensions[1].array_size_px,
        settings.dimensions[2].array_size_px,
    )

    if compression_codec is not None:
        cname = (
            "lz4"
            if compression_codec == CompressionCodec.BLOSC_LZ4
            else "zstd"
        )
        assert data.compressor.cname == cname
        assert data.compressor.clevel == 1
        assert data.compressor.shuffle == blosc.SHUFFLE
    else:
        assert data.compressor is None


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

    settings.store_path = f"{request.node.name}.zarr".replace("[", "").replace("]", "")
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
        -255,
        255,
        (
            settings.dimensions[0].chunk_size_px,
            settings.dimensions[1].array_size_px,
            settings.dimensions[2].array_size_px,
        ),
        dtype=np.int16,
    )
    stream.append(data)

    del stream  # close the stream, flush the data

    s3 = s3fs.S3FileSystem(
        key=settings.s3.access_key_id,
        secret=settings.s3.secret_access_key,
        client_kwargs={"endpoint_url": settings.s3.endpoint},
    )
    store = s3fs.S3Map(
        root=f"{s3_settings.bucket_name}/{settings.store_path}", s3=s3
    )
    cache = (
        zarr.LRUStoreCache(store, max_size=2**28)
        if version == ZarrVersion.V2
        else zarr.LRUStoreCacheV3(store, max_size=2**28)
    )
    group = zarr.group(store=cache)

    data = group["0"]

    assert data.shape == (
        settings.dimensions[0].chunk_size_px,
        settings.dimensions[1].array_size_px,
        settings.dimensions[2].array_size_px,
    )

    if compression_codec is not None:
        cname = (
            "lz4"
            if compression_codec == CompressionCodec.BLOSC_LZ4
            else "zstd"
        )
        assert data.compressor.cname == cname
        assert data.compressor.clevel == 1
        assert data.compressor.shuffle == blosc.SHUFFLE
    else:
        assert data.compressor is None

    # cleanup
    s3.rm(store.root, recursive=True)


@pytest.mark.parametrize(
    ("level",),
    [(LogLevel.DEBUG,), (LogLevel.INFO,), (LogLevel.WARNING,), (LogLevel.ERROR,), (LogLevel.NONE,)],
)
def test_set_log_level(level: LogLevel):
    set_log_level(level)
    assert get_log_level() == level
