#!/usr/bin/env python3

from pathlib import Path

import pytest

import acquire_zarr as aqz
import numpy as np


@pytest.fixture(scope="function")
def settings():
    return aqz.StreamSettings()


@pytest.fixture(scope="function")
def array_settings():
    return aqz.ArraySettings()


@pytest.fixture(scope="function")
def compression_settings():
    return aqz.CompressionSettings()


def test_settings_set_store_path(settings):
    assert settings.store_path == ""

    this_dir = str(Path(__file__).parent)
    settings.store_path = this_dir

    assert settings.store_path == this_dir


def test_set_s3_settings(settings):
    assert settings.s3 is None

    s3_settings = aqz.S3Settings(
        endpoint="foo",
        bucket_name="bar",
        region="quux",
    )
    settings.s3 = s3_settings

    assert settings.s3 is not None
    assert settings.s3.endpoint == "foo"
    assert settings.s3.bucket_name == "bar"
    assert settings.s3.region == "quux"


def test_set_compression_settings(array_settings):
    assert array_settings.compression is None

    compression_settings = aqz.CompressionSettings(
        compressor=aqz.Compressor.BLOSC1,
        codec=aqz.CompressionCodec.BLOSC_ZSTD,
        level=5,
        shuffle=2,
    )

    array_settings.compression = compression_settings
    assert array_settings.compression is not None
    assert array_settings.compression.compressor == aqz.Compressor.BLOSC1
    assert array_settings.compression.codec == aqz.CompressionCodec.BLOSC_ZSTD
    assert array_settings.compression.level == 5
    assert array_settings.compression.shuffle == 2


def test_set_dimensions(array_settings):
    assert len(array_settings.dimensions) == 0
    array_settings.dimensions = [
        aqz.Dimension(
            name="foo",
            kind=aqz.DimensionType.TIME,
            unit="nanosecond",
            scale=2.71828,
            array_size_px=1,
            chunk_size_px=2,
            shard_size_chunks=3,
        ),
        aqz.Dimension(
            name="bar",
            kind=aqz.DimensionType.SPACE,
            unit="micrometer",
            array_size_px=4,
            chunk_size_px=5,
            shard_size_chunks=6,
        ),
        aqz.Dimension(
            name="baz",
            kind=aqz.DimensionType.OTHER,
            array_size_px=7,
            chunk_size_px=8,
            shard_size_chunks=9,
        ),
    ]

    assert len(array_settings.dimensions) == 3

    assert array_settings.dimensions[0].name == "foo"
    assert array_settings.dimensions[0].kind == aqz.DimensionType.TIME
    assert array_settings.dimensions[0].unit == "nanosecond"
    assert array_settings.dimensions[0].scale == 2.71828
    assert array_settings.dimensions[0].array_size_px == 1
    assert array_settings.dimensions[0].chunk_size_px == 2
    assert array_settings.dimensions[0].shard_size_chunks == 3

    assert array_settings.dimensions[1].name == "bar"
    assert array_settings.dimensions[1].kind == aqz.DimensionType.SPACE
    assert array_settings.dimensions[1].unit == "micrometer"
    assert array_settings.dimensions[1].scale == 1.0
    assert array_settings.dimensions[1].array_size_px == 4
    assert array_settings.dimensions[1].chunk_size_px == 5
    assert array_settings.dimensions[1].shard_size_chunks == 6

    assert array_settings.dimensions[2].name == "baz"
    assert array_settings.dimensions[2].kind == aqz.DimensionType.OTHER
    assert array_settings.dimensions[2].unit is None
    assert array_settings.dimensions[2].scale == 1.0
    assert array_settings.dimensions[2].array_size_px == 7
    assert array_settings.dimensions[2].chunk_size_px == 8
    assert array_settings.dimensions[2].shard_size_chunks == 9


def test_append_dimensions(array_settings):
    assert len(array_settings.dimensions) == 0

    array_settings.dimensions.append(
        aqz.Dimension(
            name="foo",
            kind=aqz.DimensionType.TIME,
            array_size_px=1,
            chunk_size_px=2,
            shard_size_chunks=3,
        )
    )
    assert len(array_settings.dimensions) == 1
    assert array_settings.dimensions[0].name == "foo"
    assert array_settings.dimensions[0].kind == aqz.DimensionType.TIME
    assert array_settings.dimensions[0].array_size_px == 1
    assert array_settings.dimensions[0].chunk_size_px == 2
    assert array_settings.dimensions[0].shard_size_chunks == 3

    array_settings.dimensions.append(
        aqz.Dimension(
            name="bar",
            kind=aqz.DimensionType.SPACE,
            array_size_px=4,
            chunk_size_px=5,
            shard_size_chunks=6,
        )
    )
    assert len(array_settings.dimensions) == 2
    assert array_settings.dimensions[1].name == "bar"
    assert array_settings.dimensions[1].kind == aqz.DimensionType.SPACE
    assert array_settings.dimensions[1].array_size_px == 4
    assert array_settings.dimensions[1].chunk_size_px == 5
    assert array_settings.dimensions[1].shard_size_chunks == 6

    array_settings.dimensions.append(
        aqz.Dimension(
            name="baz",
            kind=aqz.DimensionType.OTHER,
            array_size_px=7,
            chunk_size_px=8,
            shard_size_chunks=9,
        )
    )
    assert len(array_settings.dimensions) == 3
    assert array_settings.dimensions[2].name == "baz"
    assert array_settings.dimensions[2].kind == aqz.DimensionType.OTHER
    assert array_settings.dimensions[2].array_size_px == 7
    assert array_settings.dimensions[2].chunk_size_px == 8
    assert array_settings.dimensions[2].shard_size_chunks == 9


def test_set_dimensions_in_constructor():
    settings = aqz.ArraySettings(
        dimensions=[
            aqz.Dimension(
                name="foo",
                kind=aqz.DimensionType.TIME,
                array_size_px=1,
                chunk_size_px=2,
                shard_size_chunks=3,
            ),
            aqz.Dimension(
                name="bar",
                kind=aqz.DimensionType.SPACE,
                array_size_px=4,
                chunk_size_px=5,
                shard_size_chunks=6,
            ),
            aqz.Dimension(
                name="baz",
                kind=aqz.DimensionType.OTHER,
                array_size_px=7,
                chunk_size_px=8,
                shard_size_chunks=9,
            ),
        ]
    )

    assert len(settings.dimensions) == 3

    assert settings.dimensions[0].name == "foo"
    assert settings.dimensions[0].kind == aqz.DimensionType.TIME
    assert settings.dimensions[0].array_size_px == 1
    assert settings.dimensions[0].chunk_size_px == 2
    assert settings.dimensions[0].shard_size_chunks == 3

    assert settings.dimensions[1].name == "bar"
    assert settings.dimensions[1].kind == aqz.DimensionType.SPACE
    assert settings.dimensions[1].array_size_px == 4
    assert settings.dimensions[1].chunk_size_px == 5
    assert settings.dimensions[1].shard_size_chunks == 6

    assert settings.dimensions[2].name == "baz"
    assert settings.dimensions[2].kind == aqz.DimensionType.OTHER
    assert settings.dimensions[2].array_size_px == 7
    assert settings.dimensions[2].chunk_size_px == 8
    assert settings.dimensions[2].shard_size_chunks == 9


def test_set_version(settings):
    assert settings.version == aqz.ZarrVersion.V3

    settings.version = aqz.ZarrVersion.V2
    assert settings.version == aqz.ZarrVersion.V2


def test_set_max_threads(settings):
    assert (
        settings.max_threads > 0
    )  # depends on your system, but will be nonzero

    settings.max_threads = 4
    assert settings.max_threads == 4


def test_set_clevel(compression_settings):
    assert compression_settings.level == 1

    compression_settings.level = 6
    assert compression_settings.level == 6


@pytest.mark.parametrize(
    ("data_type", "expected_data_type"),
    [
        (np.uint8, aqz.DataType.UINT8),
        (np.uint16, aqz.DataType.UINT16),
        (np.uint32, aqz.DataType.UINT32),
        (np.uint64, aqz.DataType.UINT64),
        (np.int8, aqz.DataType.INT8),
        (np.int16, aqz.DataType.INT16),
        (np.int32, aqz.DataType.INT32),
        (np.int64, aqz.DataType.INT64),
        (np.float32, aqz.DataType.FLOAT32),
        (np.float64, aqz.DataType.FLOAT64),
    ],
)
def test_set_dtype(
    array_settings, data_type: np.dtype, expected_data_type: aqz.DataType
):
    assert array_settings.data_type == aqz.DataType.UINT8

    array_settings.data_type = data_type
    assert array_settings.data_type == expected_data_type


def test_estimate_max_memory_usage():
    array = aqz.ArraySettings()
    array.dimensions = [
        aqz.Dimension(name="t", kind=aqz.DimensionType.TIME, array_size_px=0, chunk_size_px=5),
        aqz.Dimension(name="c", kind=aqz.DimensionType.CHANNEL, array_size_px=3, chunk_size_px=1),
        aqz.Dimension(name="z", kind=aqz.DimensionType.SPACE, array_size_px=6, chunk_size_px=2),
        aqz.Dimension(name="y", kind=aqz.DimensionType.SPACE, array_size_px=48, chunk_size_px=16),
        aqz.Dimension(name="x", kind=aqz.DimensionType.SPACE, array_size_px=64, chunk_size_px=16),
    ]
    array.data_type = np.uint16

    array_usage = np.dtype(np.uint16).itemsize * array.dimensions[0].chunk_size_px
    for dim in array.dimensions[1:]:
        array_usage *= dim.array_size_px
    frame_queue_usage = 1 << 30
    frame_buffer_usage = array.dimensions[-2].array_size_px * array.dimensions[-1].array_size_px * np.dtype(
        np.uint16).itemsize
    expected_memory = array_usage + frame_buffer_usage + frame_queue_usage

    stream = aqz.StreamSettings(
        arrays=[array]
    )
    max_memory = stream.get_maximum_memory_usage()

    assert max_memory == expected_memory
