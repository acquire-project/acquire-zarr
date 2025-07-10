"""

Acquire Zarr Writer Python API
-----------------------
.. currentmodule:: acquire_zarr
.. autosummary::
   :toctree: _generate
   append

"""

from __future__ import annotations
import numpy
from typing import Any, ClassVar, List, Optional

__all__ = [
    "ArraySettings",
    "CompressionCodec",
    "CompressionSettings",
    "Compressor",
    "DataType",
    "Dimension",
    "DimensionType",
    "DownsamplingMethod",
    "LogLevel",
    "S3Settings",
    "StreamSettings",
    "ZarrStream",
    "ZarrVersion",
    "get_log_level",
    "set_log_level",
]


class ArraySettings:
    """Settings for a single array in the Zarr stream.

    Attributes:
      output_key: Key within the Zarr dataset where this array will be stored.
      dimensions: List of dimension properties defining the dataset structure. Should be ordered from slowest to fastest changing (e.g., [Z, Y, X] for 3D data).
      data_type: The pixel data type for the dataset.
      compression: Optional compression settings for chunks. If None, no compression is applied.
      downsampling_method: Method used for generating optional multiscale levels (image pyramid).
    """

    output_key: str
    dimensions: List[Dimension]
    data_type: DataType
    compression: Optional[CompressionSettings]
    downsampling_method: Optional[DownsamplingMethod]

    def __init__(self, **kwargs) -> None: ...

    def __repr__(self) -> str: ...


class CompressionCodec:
    """Codec to use for compression, if any.

    Attributes:
      NONE: No compression
      BLOSC_LZ4: LZ4 compression using Blosc
      BLOSC_ZSTD: Zstd compression using Blosc
    """

    NONE: ClassVar[CompressionCodec]  # value = <CompressionCodec.NONE: 0>
    BLOSC_LZ4: ClassVar[
        CompressionCodec
    ]  # value = <CompressionCodec.BLOSC_LZ4: 1>
    BLOSC_ZSTD: ClassVar[
        CompressionCodec
    ]  # value = <CompressionCodec.BLOSC_ZSTD: 2>
    __members__: ClassVar[
        dict[str, CompressionCodec]
    ]  # value = {'NONE': <CompressionCodec.NONE: 0>, 'BLOSC_LZ4': <CompressionCodec.BLOSC_LZ4: 1>, 'BLOSC_ZSTD': <CompressionCodec.BLOSC_ZSTD: 2>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


class CompressionSettings:
    """Settings for compressing during acquisition."""

    codec: CompressionCodec
    compressor: Compressor
    level: int
    shuffle: int

    def __init__(self, **kwargs) -> None: ...

    def __repr__(self) -> str: ...


class Compressor:
    """
    Compressor to use, if any.

    Attributes:
      NONE: No compression.
      BLOSC1: Blosc compressor.
    """

    NONE: ClassVar[Compressor]  # value = <Compressor.NONE: 0>
    BLOSC1: ClassVar[Compressor]  # value = <Compressor.BLOSC1: 1>
    __members__: ClassVar[
        dict[str, Compressor]
    ]  # value = {'NONE': <Compressor.NONE: 0>, 'BLOSC1': <Compressor.BLOSC1: 1>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


class DataType:
    """
    Data type used in the stream.

    Attributes:
      UINT8: Unsigned 8-bit integer.
      UINT16: Unsigned 16-bit integer.
      UINT32: Unsigned 32-bit integer.
      UINT64: Unsigned 64-bit integer.
      INT8: Signed 8-bit integer.
      INT16: Signed 16-bit integer.
      INT32: Signed 32-bit integer.
      INT64: Signed 64-bit integer.
      FLOAT32: Single precision floating point.
      FLOAT64: Double precision floating point.
    """

    UINT8: ClassVar[DataType]  # value = <DataType.UINT8: 0>
    UINT16: ClassVar[DataType]  # value = <DataType.UINT16: 1>
    UINT32: ClassVar[DataType]  # value = <DataType.UINT32: 2>
    UINT64: ClassVar[DataType]  # value = <DataType.UINT64: 3>
    INT8: ClassVar[DataType]  # value = <DataType.INT8: 4>
    INT16: ClassVar[DataType]  # value = <DataType.INT16: 5>
    INT32: ClassVar[DataType]  # value = <DataType.INT32: 6>
    INT64: ClassVar[DataType]  # value = <DataType.INT64: 7>
    FLOAT32: ClassVar[DataType]  # value = <DataType.FLOAT32: 8>
    FLOAT64: ClassVar[DataType]  # value = <DataType.FLOAT64: 9>
    __members__: ClassVar[
        dict[str, DataType]
    ]  # value = {'UINT8': <DataType.UINT8: 0>, 'UINT16': <DataType.UINT16: 1>, 'UINT32': <DataType.UINT32: 2>, 'UINT64': <DataType.UINT64: 3>, 'INT8': <DataType.INT8: 4>, 'INT16': <DataType.INT16: 5>, 'INT32': <DataType.INT32: 6>, 'INT64': <DataType.INT64: 7>, 'FLOAT32': <DataType.FLOAT32: 8>, 'FLOAT64': <DataType.FLOAT64: 9>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


class Dimension:
    """Properties of a dimension of the output array."""

    array_size_px: int
    chunk_size_px: int
    kind: DimensionType
    name: str
    scale: float
    shard_size_chunks: int
    unit: Optional[str]

    def __init__(self, **kwargs) -> None: ...

    def __repr__(self) -> str: ...


class DimensionType:
    """
    Type of dimension.

    Attributes:
      SPACE: Spatial dimension.
      CHANNEL: Channel dimension.
      TIME: Time dimension.
      OTHER: Other dimension.
    """

    SPACE: ClassVar[DimensionType]  # value = <DimensionType.SPACE: 0>
    CHANNEL: ClassVar[DimensionType]  # value = <DimensionType.CHANNEL: 1>
    TIME: ClassVar[DimensionType]  # value = <DimensionType.TIME: 2>
    OTHER: ClassVar[DimensionType]  # value = <DimensionType.OTHER: 3>
    __members__: ClassVar[
        dict[str, DimensionType]
    ]  # value = {'SPACE': <DimensionType.SPACE: 0>, 'CHANNEL': <DimensionType.CHANNEL: 1>, 'TIME': <DimensionType.TIME: 2>, 'OTHER': <DimensionType.OTHER: 3>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


class DownsamplingMethod:
    """
    Method used to downsample frames.

    Attributes:
      DECIMATE: Take the top left of each 4x4 block of pixels
      MEAN: Take the mean value of each 4x4 block of pixels
      MIN: Take the minimum value of each 4x4 block of pixels
      MAX: Take the maximum value of each 4x4 block of pixels
    """

    DECIMATE: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.DECIMATE: 0>
    MEAN: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.MEAN: 1>
    MIN: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.MIN: 2>
    MAX: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.MAX: 3>
    __members__: ClassVar[
        dict[str, DownsamplingMethod]
    ]  # value = {'DECIMATE': <DownsamplingMethod.DECIMATE: 0>, 'MEAN': <DownsamplingMethod.MEAN: 1>, 'MIN': <DownsamplingMethod.MIN: 2>, 'MAX': <DownsamplingMethod.MAX: 3>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


class LogLevel:
    """
    Severity level to filter logs by.

    Attributes:
      DEBUG: Detailed information for debugging purposes.
      INFO: Informational messages.
      WARNING: Warnings.
      ERROR: Errors.
      NONE: Disable logging.
    """

    DEBUG: ClassVar[LogLevel]  # value = <LogLevel.DEBUG: 0>
    INFO: ClassVar[LogLevel]  # value = <LogLevel.INFO: 1>
    WARNING: ClassVar[LogLevel]  # value = <LogLevel.WARNING: 2>
    ERROR: ClassVar[LogLevel]  # value = <LogLevel.ERROR: 3>
    NONE: ClassVar[LogLevel]  # value = <LogLevel.NONE: 4>
    __members__: ClassVar[
        dict[str, LogLevel]
    ]  # value = {'DEBUG': <LogLevel.DEBUG: 0>, 'INFO': <LogLevel.INFO: 1>, 'WARNING': <LogLevel.WARNING: 2>, 'ERROR': <LogLevel.ERROR: 3>, 'NONE': <LogLevel.NONE: 4>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


class S3Settings:
    """Settings for connecting to and storing data in S3."""

    bucket_name: str
    endpoint: str
    region: Optional[str]

    def __init__(self, **kwargs) -> None: ...

    def __repr__(self) -> str: ...


class StreamSettings:
    """Settings for configuring a Zarr stream.

    This class encapsulates all the configuration options needed to create a Zarr stream,
    including storage location, array configuration, and format options.

    Attributes:
        arrays: List of ArraySettings defining the structure and properties of each array in the dataset.
        store_path: Path to the store. Can be a filesystem path or S3 key prefix.
            For S3, this becomes the key prefix within the specified bucket.
        s3: Optional S3 settings for cloud storage. If None, writes to local filesystem.
        version: Zarr format version to use (V2 or V3).
        max_threads: Maximum number of threads for parallel processing.
        custom_metadata: Optional JSON-formatted custom metadata to include in the dataset.
        overwrite: If True, removes any existing data at store_path before writing.

    Note:
        For S3 storage with endpoint "s3://my-endpoint.com", bucket "my-bucket", and
        store_path "my-dataset.zarr", the final location will be
        "s3://my-endpoint.com/my-bucket/my-dataset.zarr".
    """

    arrays: List[ArraySettings]
    custom_metadata: Optional[str]
    s3: Optional[S3Settings]
    store_path: str
    version: ZarrVersion
    max_threads: int
    overwrite: bool

    def __init__(self, **kwargs) -> None: ...

    def __repr__(self) -> str: ...


class ZarrStream:
    def __init__(self, arg0: StreamSettings) -> None: ...

    def append(self, data: numpy.ndarray, key: Optional[str]=None) -> None: ...

    def write_custom_metadata(
            self, metadata: str, overwrite: bool = False
    ) -> bool: ...

    def is_active(self) -> bool: ...
    def close(self) -> None: ...


class ZarrVersion:
    """
    Zarr format version.

    Attributes:
      V2: Zarr format version 2
      V3: Zarr format version 3
    """

    V2: ClassVar[ZarrVersion]  # value = <ZarrVersion.V2: 2>
    V3: ClassVar[ZarrVersion]  # value = <ZarrVersion.V3: 3>
    __members__: ClassVar[
        dict[str, ZarrVersion]
    ]  # value = {'V2': <ZarrVersion.V2: 2>, 'V3': <ZarrVersion.V3: 3>}

    def __eq__(self, other: Any) -> bool: ...

    def __getstate__(self) -> int: ...

    def __hash__(self) -> int: ...

    def __index__(self) -> int: ...

    def __init__(self, value: int) -> None: ...

    def __int__(self) -> int: ...

    def __ne__(self, other: Any) -> bool: ...

    def __repr__(self) -> str: ...

    def __setstate__(self, state: int) -> None: ...

    def __str__(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def value(self) -> int: ...


def get_log_level() -> LogLevel:
    """Get the current log level for the Zarr API"""


def set_log_level(level: LogLevel) -> None:
    """Set the log level for the Zarr API"""
