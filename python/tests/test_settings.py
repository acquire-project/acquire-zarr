#!/usr/bin/env python3

import json
from pathlib import Path

import numpy as np
import pytest

import acquire_zarr


@pytest.fixture(scope="function")
def settings():
    return acquire_zarr.ZarrStreamSettings()


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

    s3_settings = acquire_zarr.ZarrS3Settings(
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

    compression_settings = acquire_zarr.ZarrCompressionSettings(
        compressor=acquire_zarr.ZarrCompressor.BLOSC1,
        codec=acquire_zarr.ZarrCompressionCodec.BLOSC_ZSTD,
        level=5,
        shuffle=2,
    )

    settings.compression = compression_settings
    assert settings.compression is not None
    assert settings.compression.compressor == acquire_zarr.ZarrCompressor.BLOSC1
    assert settings.compression.codec == acquire_zarr.ZarrCompressionCodec.BLOSC_ZSTD
    assert settings.compression.level == 5
    assert settings.compression.shuffle == 2


def test_set_multiscale(settings):
    assert settings.multiscale is False

    settings.multiscale = True

    assert settings.multiscale is True


def test_set_version(settings):
    assert settings.version == acquire_zarr.ZarrVersion.V2

    settings.version = acquire_zarr.ZarrVersion.V3

    assert settings.version == acquire_zarr.ZarrVersion.V3
