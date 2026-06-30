#!/usr/bin/env python3

from pathlib import Path

import pytest

import acquire_zarr as aqz
import numpy as np

CONFIGS = {
    "yaml": """
version: 1
store_path: from-config.zarr
overwrite: true
max_threads: 4
arrays:
  - output_key: channel0
    data_type: uint16
    multiscale: true
    downsampling_method: mean
    compression:
      compressor: blosc1
      codec: blosc-zstd
      level: 1
      shuffle: 1
    dimensions:
      - {name: t, type: time,  array_size_px: 0,  chunk_size_px: 1,  shard_size_chunks: 1}
      - {name: z, type: space, array_size_px: 10, chunk_size_px: 5,  shard_size_chunks: 1}
      - {name: y, type: space, array_size_px: 48, chunk_size_px: 16, shard_size_chunks: 1, unit: micrometer, scale: 0.5}
      - {name: x, type: space, array_size_px: 64, chunk_size_px: 16, shard_size_chunks: 1, unit: micrometer, scale: 0.5}
""",
    "json": """
{
  "version": 1,
  "store_path": "from-config.zarr",
  "overwrite": true,
  "max_threads": 4,
  "arrays": [
    {
      "output_key": "channel0",
      "data_type": "uint16",
      "multiscale": true,
      "downsampling_method": "mean",
      "compression": {"compressor": "blosc1", "codec": "blosc-zstd", "level": 1, "shuffle": 1},
      "dimensions": [
        {"name": "t", "type": "time",  "array_size_px": 0,  "chunk_size_px": 1,  "shard_size_chunks": 1},
        {"name": "z", "type": "space", "array_size_px": 10, "chunk_size_px": 5,  "shard_size_chunks": 1},
        {"name": "y", "type": "space", "array_size_px": 48, "chunk_size_px": 16, "shard_size_chunks": 1, "unit": "micrometer", "scale": 0.5},
        {"name": "x", "type": "space", "array_size_px": 64, "chunk_size_px": 16, "shard_size_chunks": 1, "unit": "micrometer", "scale": 0.5}
      ]
    }
  ]
}
""",
}


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

    with pytest.raises(RuntimeError):
        settings.version = 2  # only V3 is supported


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
        aqz.Dimension(
            name="t",
            kind=aqz.DimensionType.TIME,
            array_size_px=0,
            chunk_size_px=5,
        ),
        aqz.Dimension(
            name="c",
            kind=aqz.DimensionType.CHANNEL,
            array_size_px=3,
            chunk_size_px=1,
        ),
        aqz.Dimension(
            name="z",
            kind=aqz.DimensionType.SPACE,
            array_size_px=6,
            chunk_size_px=2,
        ),
        aqz.Dimension(
            name="y",
            kind=aqz.DimensionType.SPACE,
            array_size_px=48,
            chunk_size_px=16,
        ),
        aqz.Dimension(
            name="x",
            kind=aqz.DimensionType.SPACE,
            array_size_px=64,
            chunk_size_px=16,
        ),
    ]
    array.data_type = np.uint16

    array_usage = (
        np.dtype(np.uint16).itemsize * array.dimensions[0].chunk_size_px
    )
    for dim in array.dimensions[1:]:
        array_usage *= dim.array_size_px
    frame_buffer_usage = (
        array.dimensions[-2].array_size_px
        * array.dimensions[-1].array_size_px
        * np.dtype(np.uint16).itemsize
    )
    # mirror init_frame_queue_: 256 MiB budget clamped to [16, 512] frames
    frame_queue_frames = min(max((256 << 20) // frame_buffer_usage, 16), 512)
    frame_queue_usage = frame_queue_frames * frame_buffer_usage
    expected_memory = array_usage + frame_buffer_usage + frame_queue_usage

    stream = aqz.StreamSettings(arrays=[array])
    max_memory = stream.get_maximum_memory_usage()

    assert max_memory == expected_memory


def _assert_expected(s):
    assert s.store_path == "from-config.zarr"
    assert s.overwrite is True
    assert s.max_threads == 4
    assert len(s.arrays) == 1
    a = s.arrays[0]
    assert a.output_key == "channel0"
    assert a.data_type == aqz.DataType.UINT16
    assert a.downsampling_method == aqz.DownsamplingMethod.MEAN
    assert a.compression is not None
    assert a.compression.compressor == aqz.Compressor.BLOSC1
    assert a.compression.codec == aqz.CompressionCodec.BLOSC_ZSTD
    assert a.compression.level == 1
    assert a.compression.shuffle == 1
    assert len(a.dimensions) == 4
    assert a.dimensions[0].name == "t"
    assert a.dimensions[0].kind == aqz.DimensionType.TIME
    assert a.dimensions[2].name == "y"
    assert a.dimensions[2].unit == "micrometer"
    assert a.dimensions[2].scale == 0.5


@pytest.mark.parametrize("fmt", ["yaml", "json"])
def test_load_settings_from_string(fmt):
    _assert_expected(aqz.StreamSettings.from_string(CONFIGS[fmt]))


def test_config_round_trip(tmp_path):
    base = aqz.StreamSettings.from_string(CONFIGS["yaml"])

    # dump -> reload through both formats
    _assert_expected(aqz.StreamSettings.from_string(base.to_yaml()))
    _assert_expected(aqz.StreamSettings.from_string(base.to_json()))

    # dump to file (format by extension) -> reload
    for name in ("rt.yaml", "rt.json"):
        path = tmp_path / name
        base.to_file(str(path))
        _assert_expected(aqz.StreamSettings.from_file(str(path)))


def test_load_settings_rejects_malformed():
    with pytest.raises(ValueError):
        aqz.StreamSettings.from_string(
            "version: 1\nstore_path: x\n"
        )  # no arrays
    with pytest.raises(ValueError):
        aqz.StreamSettings.from_string(
            "store_path: x\narrays:\n  - data_type: float128\n    dimensions: []\n"
        )


def test_config_dict_round_trip():
    base = aqz.StreamSettings.from_string(CONFIGS["yaml"])

    d = base.to_dict()
    assert isinstance(d, dict)
    assert d["store_path"] == "from-config.zarr"
    assert d["arrays"][0]["data_type"] == "uint16"

    _assert_expected(aqz.StreamSettings.from_dict(d))


def test_yaml_dump_quotes_ambiguous_strings():
    hcs_yaml = """
version: 1
store_path: plate.zarr
plates:
  - path: test_plate
    name: Test Plate
    row_names: [C]
    column_names: ["5"]
    wells:
      - row_name: C
        column_name: "5"
        images:
          - path: fov1
            array:
              data_type: uint16
              dimensions:
                - {name: z, type: space, array_size_px: 0,  chunk_size_px: 1,  shard_size_chunks: 1}
                - {name: y, type: space, array_size_px: 64, chunk_size_px: 64, shard_size_chunks: 1}
                - {name: x, type: space, array_size_px: 64, chunk_size_px: 64, shard_size_chunks: 1}
"""

    s = aqz.StreamSettings.from_string(hcs_yaml)
    assert s.hcs_plates[0].wells[0].column_name == "5"

    yaml = s.to_yaml()
    # numeric-looking names are emitted quoted so they reload as strings
    assert '"5"' in yaml

    reloaded = aqz.StreamSettings.from_string(yaml)
    assert reloaded.hcs_plates[0].wells[0].column_name == "5"
    assert list(reloaded.hcs_plates[0].column_names) == ["5"]
