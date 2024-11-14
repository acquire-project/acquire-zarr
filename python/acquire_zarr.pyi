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

__all__ = ['CompressionCodec', 'CompressionSettings', 'Compressor', 'DataType', 'Dimension', 'DimensionType',
           'LogLevel', 'S3Settings', 'StreamSettings', 'ZarrStream', 'ZarrVersion', 'get_log_level',
           'set_log_level']


class CompressionCodec:
    """Codec to use for compression, if any.
    Members:
      NONE: No compression
      BLOSC_LZ4: LZ4 compression using Blosc
      BLOSC_ZSTD: Zstd compression using Blosc
    """
    NONE: ClassVar[CompressionCodec]  # value = <CompressionCodec.NONE: 0>
    BLOSC_LZ4: ClassVar[CompressionCodec]  # value = <CompressionCodec.BLOSC_LZ4: 1>
    BLOSC_ZSTD: ClassVar[CompressionCodec]  # value = <CompressionCodec.BLOSC_ZSTD: 2>
    __members__: ClassVar[dict[
        str, CompressionCodec]]  # value = {'NONE': <CompressionCodec.NONE: 0>, 'BLOSC_LZ4': <CompressionCodec.BLOSC_LZ4: 1>, 'BLOSC_ZSTD': <CompressionCodec.BLOSC_ZSTD: 2>}

    def __eq__(self, other: Any) -> bool:
        ...

    def __getstate__(self) -> int:
        ...

    def __hash__(self) -> int:
        ...

    def __index__(self) -> int:
        ...

    def __init__(self, value: int) -> None:
        ...

    def __int__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def __setstate__(self, state: int) -> None:
        ...

    def __str__(self) -> str:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def value(self) -> int:
        ...


class CompressionSettings:
    """Settings for compressing during acquisition."""
    codec: CompressionCodec
    compressor: Compressor
    level: int
    shuffle: int

    def __init__(self, **kwargs) -> None:
        ...

    def __repr__(self) -> str:
        ...


class Compressor:
    """
    Compressor to use, if any.

    Members:
      NONE: No compression.
      BLOSC1: Blosc compressor.
    """
    NONE: ClassVar[Compressor]  # value = <Compressor.NONE: 0>
    BLOSC1: ClassVar[Compressor]  # value = <Compressor.BLOSC1: 1>
    __members__: ClassVar[
        dict[str, Compressor]]  # value = {'NONE': <Compressor.NONE: 0>, 'BLOSC1': <Compressor.BLOSC1: 1>}

    def __eq__(self, other: Any) -> bool:
        ...

    def __getstate__(self) -> int:
        ...

    def __hash__(self) -> int:
        ...

    def __index__(self) -> int:
        ...

    def __init__(self, value: int) -> None:
        ...

    def __int__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def __setstate__(self, state: int) -> None:
        ...

    def __str__(self) -> str:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def value(self) -> int:
        ...


class DataType:
    """
    Data type used in the stream.

    Members:
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
    __members__: ClassVar[dict[
        str, DataType]]  # value = {'UINT8': <DataType.UINT8: 0>, 'UINT16': <DataType.UINT16: 1>, 'UINT32': <DataType.UINT32: 2>, 'UINT64': <DataType.UINT64: 3>, 'INT8': <DataType.INT8: 4>, 'INT16': <DataType.INT16: 5>, 'INT32': <DataType.INT32: 6>, 'INT64': <DataType.INT64: 7>, 'FLOAT32': <DataType.FLOAT32: 8>, 'FLOAT64': <DataType.FLOAT64: 9>}

    def __eq__(self, other: Any) -> bool:
        ...

    def __getstate__(self) -> int:
        ...

    def __hash__(self) -> int:
        ...

    def __index__(self) -> int:
        ...

    def __init__(self, value: int) -> None:
        ...

    def __int__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def __setstate__(self, state: int) -> None:
        ...

    def __str__(self) -> str:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def value(self) -> int:
        ...


class Dimension:
    """Properties of a dimension of the output array."""
    array_size_px: int
    chunk_size_px: int
    kind: DimensionType
    name: str
    shard_size_chunks: int

    def __init__(self, **kwargs) -> None:
        ...

    def __repr__(self) -> str:
        ...


class DimensionType:
    """
    Type of dimension.
    Members:
      SPACE: Spatial dimension.
      CHANNEL: Channel dimension.
      TIME: Time dimension.
      OTHER: Other dimension.
    """
    SPACE: ClassVar[DimensionType]  # value = <DimensionType.SPACE: 0>
    CHANNEL: ClassVar[DimensionType]  # value = <DimensionType.CHANNEL: 1>
    TIME: ClassVar[DimensionType]  # value = <DimensionType.TIME: 2>
    OTHER: ClassVar[DimensionType]  # value = <DimensionType.OTHER: 3>
    __members__: ClassVar[dict[
        str, DimensionType]]  # value = {'SPACE': <DimensionType.SPACE: 0>, 'CHANNEL': <DimensionType.CHANNEL: 1>, 'TIME': <DimensionType.TIME: 2>, 'OTHER': <DimensionType.OTHER: 3>}

    def __eq__(self, other: Any) -> bool:
        ...

    def __getstate__(self) -> int:
        ...

    def __hash__(self) -> int:
        ...

    def __index__(self) -> int:
        ...

    def __init__(self, value: int) -> None:
        ...

    def __int__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def __setstate__(self, state: int) -> None:
        ...

    def __str__(self) -> str:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def value(self) -> int:
        ...


class LogLevel:
    """
    Severity level to filter logs by.
    Members:
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
    __members__: ClassVar[dict[
        str, LogLevel]]  # value = {'DEBUG': <LogLevel.DEBUG: 0>, 'INFO': <LogLevel.INFO: 1>, 'WARNING': <LogLevel.WARNING: 2>, 'ERROR': <LogLevel.ERROR: 3>, 'NONE': <LogLevel.NONE: 4>}

    def __eq__(self, other: Any) -> bool:
        ...

    def __getstate__(self) -> int:
        ...

    def __hash__(self) -> int:
        ...

    def __index__(self) -> int:
        ...

    def __init__(self, value: int) -> None:
        ...

    def __int__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def __setstate__(self, state: int) -> None:
        ...

    def __str__(self) -> str:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def value(self) -> int:
        ...


class S3Settings:
    """Settings for connecting to and storing data in S3."""
    access_key_id: str
    bucket_name: str
    endpoint: str
    secret_access_key: str

    def __init__(self, **kwargs) -> None:
        ...

    def __repr__(self) -> str:
        ...


class StreamSettings:
    """Settings for the Zarr stream."""
    compression: Optional[CompressionSettings]
    custom_metadata: Optional[str]
    data_type: DataType
    dimensions: List[Dimension]
    multiscale: bool
    s3: Optional[S3Settings]
    store_path: str
    version: ZarrVersion

    def __init__(self, **kwargs) -> None:
        ...

    def __repr__(self) -> str:
        ...


class ZarrStream:
    def __init__(self, arg0: StreamSettings) -> None:
        ...

    def append(self, arg0: numpy.ndarray) -> None:
        ...

    def is_active(self) -> bool:
        ...


class ZarrVersion:
    """
    Zarr format version.
    Members:
      V2: Zarr format version 2
      V3: Zarr format version 3
    """
    V2: ClassVar[ZarrVersion]  # value = <ZarrVersion.V2: 2>
    V3: ClassVar[ZarrVersion]  # value = <ZarrVersion.V3: 3>
    __members__: ClassVar[
        dict[str, ZarrVersion]]  # value = {'V2': <ZarrVersion.V2: 2>, 'V3': <ZarrVersion.V3: 3>}

    def __eq__(self, other: Any) -> bool:
        ...

    def __getstate__(self) -> int:
        ...

    def __hash__(self) -> int:
        ...

    def __index__(self) -> int:
        ...

    def __init__(self, value: int) -> None:
        ...

    def __int__(self) -> int:
        ...

    def __ne__(self, other: Any) -> bool:
        ...

    def __repr__(self) -> str:
        ...

    def __setstate__(self, state: int) -> None:
        ...

    def __str__(self) -> str:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def value(self) -> int:
        ...


def get_log_level() -> LogLevel:
    """Get the current log level for the Zarr API"""


def set_log_level(level: LogLevel) -> None:
    """Set the log level for the Zarr API"""
