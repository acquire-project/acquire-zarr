#!/usr/bin/env python3

import json
from pathlib import Path

import pytest

import acquire_zarr


@pytest.fixture(scope="function")
def settings():
    return acquire_zarr.StreamSettings()


def test_settings_set_store_path(settings):
    assert settings.store_path == ""

    this_dir = str(Path(__file__).parent)
    settings.store_path = this_dir

    assert settings.store_path == this_dir


def test_settings_set_custom_metadata(settings):
    assert settings.custom_metadata is None

    metadata = json.dumps({"foo": "bar"})
    settings.custom_metadata = metadata

    assert settings.custom_metadata == metadata


def test_set_s3_settings(settings):
    assert settings.s3 is None

    s3_settings = acquire_zarr.S3Settings(
        endpoint="foo",
        bucket_name="bar",
        access_key_id="baz",
        secret_access_key="qux",
    )
    settings.s3 = s3_settings

    assert settings.s3 is not None
    assert settings.s3.endpoint == "foo"
    assert settings.s3.bucket_name == "bar"
    assert settings.s3.access_key_id == "baz"
    assert settings.s3.secret_access_key == "qux"


def test_set_compression_settings(settings):
    assert settings.compression is None

    compression_settings = acquire_zarr.CompressionSettings(
        compressor=acquire_zarr.Compressor.BLOSC1,
        codec=acquire_zarr.CompressionCodec.BLOSC_ZSTD,
        level=5,
        shuffle=2,
    )

    settings.compression = compression_settings
    assert settings.compression is not None
    assert settings.compression.compressor == acquire_zarr.Compressor.BLOSC1
    assert settings.compression.codec == acquire_zarr.CompressionCodec.BLOSC_ZSTD
    assert settings.compression.level == 5
    assert settings.compression.shuffle == 2


def test_set_dimensions(settings):
    assert len(settings.dimensions) == 0

    settings.dimensions.append(acquire_zarr.Dimension(
        name="foo",
        kind=acquire_zarr.DimensionType.TIME,
        array_size_px=1,
        chunk_size_px=2,
        shard_size_chunks=3,
    ))
    assert len(settings.dimensions) == 1
    assert settings.dimensions[0].name == "foo"
    assert settings.dimensions[0].kind == acquire_zarr.DimensionType.TIME
    assert settings.dimensions[0].array_size_px == 1
    assert settings.dimensions[0].chunk_size_px == 2
    assert settings.dimensions[0].shard_size_chunks == 3

    settings.dimensions.append(acquire_zarr.Dimension(
        name="bar",
        kind=acquire_zarr.DimensionType.SPACE,
        array_size_px=4,
        chunk_size_px=5,
        shard_size_chunks=6,
    ))
    assert len(settings.dimensions) == 2
    assert settings.dimensions[1].name == "bar"
    assert settings.dimensions[1].kind == acquire_zarr.DimensionType.SPACE
    assert settings.dimensions[1].array_size_px == 4
    assert settings.dimensions[1].chunk_size_px == 5
    assert settings.dimensions[1].shard_size_chunks == 6

    settings.dimensions.append(acquire_zarr.Dimension(
        name="baz",
        kind=acquire_zarr.DimensionType.OTHER,
        array_size_px=7,
        chunk_size_px=8,
        shard_size_chunks=9,
    ))
    assert len(settings.dimensions) == 3
    assert settings.dimensions[2].name == "baz"
    assert settings.dimensions[2].kind == acquire_zarr.DimensionType.OTHER
    assert settings.dimensions[2].array_size_px == 7
    assert settings.dimensions[2].chunk_size_px == 8
    assert settings.dimensions[2].shard_size_chunks == 9


def test_set_multiscale(settings):
    assert settings.multiscale is False

    settings.multiscale = True

    assert settings.multiscale is True


def test_set_version(settings):
    assert settings.version == acquire_zarr.ZarrVersion.V2

    settings.version = acquire_zarr.ZarrVersion.V3

    assert settings.version == acquire_zarr.ZarrVersion.V3
