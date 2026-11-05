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
from typing import Any, ClassVar, List, Optional, Union

__all__ = [
    "ArraySettings",
    "CompressionCodec",
    "CompressionSettings",
    "Compressor",
    "CustomMetadata",
    "DataType",
    "Dimension",
    "DimensionType",
    "DownsamplingMethod",
    "IntermediateGroups",
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

    The resulting Zarr hierarchy depends on ``is_ngff`` and ``downsampling_method``:

    - **is_ngff=False, no downsampling_method**: A simple array node is written.
    - **is_ngff=True, no downsampling_method**: A single-level OME-NGFF multiscales
      group is written with no image pyramid.
    - **downsampling_method set**: An OME-NGFF multiscales group is written with
      the full-resolution array at level ``0`` plus additional downsampled levels.
      The number of levels is determined automatically from the chunk and array
      sizes, and can be capped with ``max_levels``. ``is_ngff`` is coerced to True.

    In every case the node is written at ``store_path / output_key``, or at
    ``store_path`` if ``output_key`` is empty or omitted. Intermediate group
    directories are created as needed.

    Attributes:
      output_key: Path within the Zarr store where this array (or multiscales
        group) will be written, relative to ``StreamSettings.store_path``. If
        empty or omitted, the array is placed at the store root.
      dimensions: List of dimension properties defining the dataset structure.
        Should be ordered from slowest to fastest changing (e.g., [Z, Y, X] for 3D data).
      data_type: The pixel data type for the dataset.
      compression: Optional compression settings for chunks. If None, no compression is applied.
      downsampling_method: Method used for generating optional multiscale levels
        (image pyramid). When set, `is_ngff` is also coerced to True and the array
        is wrapped in an OME-NGFF multiscales group. When None (default), no
        image pyramid is generated.
      max_levels: Maximum number of downsampled pyramid levels to generate when
        ``downsampling_method`` is set. The number of levels is otherwise
        determined automatically from the chunk and array sizes. Set to 0
        (the default) for no limit.
      storage_dimension_order: Order of dimensions for storage, which may different
        from the acquisition order defined in `dimensions`. Must be a list of dimension
        names corresponding to those in `dimensions`.
        This allows reordering according to the desired final layout (e.g.,
        `[TIME, CHANNEL, Z, Y, X]`, which is currently mandated by the OME-Zarr
        specification).
        Current limitations:
            - The first dimension *may not* be moved to a non-leading position.
            - The last two dimensions *may not* be moved out of the final two positions,
              but MAY be swapped with each other.
      is_ngff: When True, the array is wrapped in an OME-NGFF multiscales group.
        When False (default), a simple array node is written. Note that setting
        `downsampling_method` will coerce this to True regardless, and
        subsequently setting `is_ngff = False` will have no effect as long as
        `downsampling_method` is set. The coercion lasts only as long as the
        downsampling method does: clearing `downsampling_method` restores the
        value last requested via `is_ngff`. Setting `is_ngff = True` without a
        downsampling method produces a single-level OME-NGFF multiscales group
        with no image pyramid.
    """

    output_key: str
    dimensions: List[Dimension]
    data_type: Union[DataType, numpy.dtype]
    compression: Optional[CompressionSettings]
    downsampling_method: Optional[DownsamplingMethod]
    max_levels: int
    storage_dimension_order: List[str]
    is_ngff: bool

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

class CustomMetadata:
    """Custom metadata that the stream writes when it is created.

    The stream writes each entry of ``StreamSettings.custom_metadata`` in
    order, as ``ZarrStream.write_custom_metadata`` does, before the stream is
    returned. If an entry fails, creating the stream fails. Each entry's node,
    JSON and keys are checked before the store is changed; a level of detail
    that does not exist is found only after.

    Attributes:
        metadata: The metadata. A ``str`` is taken as JSON text, as in
            ``write_custom_metadata``; any other value is serialized with
            ``json.dumps``. Reading it gives the parsed value.
        node_key: Key of the node, as for ``write_custom_metadata``. None is
            the stream's only array.
        metadata_key: Key under ``attributes``, or None to write each key of
            ``metadata`` there.
    """

    metadata: Any
    node_key: Optional[str]
    metadata_key: Optional[str]

    def __init__(
        self,
        metadata: Any,
        *,
        node_key: Optional[str] = None,
        metadata_key: Optional[str] = None,
    ) -> None: ...
    def __repr__(self) -> str: ...

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

    There is deliberately no ``NONE`` member: "no downsampling" is spelled
    ``None``, since ``ArraySettings.downsampling_method`` is an
    ``Optional[DownsamplingMethod]``. The C enum does carry a
    ``ZarrDownsamplingMethod_None`` zero value, which maps to ``None`` here.

    Attributes:
      DECIMATE: Take the top left of each 2x2 block of pixels
      MEAN: Take the mean value of each 2x2 block of pixels. Integer types
        truncate the mean toward zero. In 3D, the 2x2 mean of each plane is
        truncated before the two planes are averaged.
      MIN: Take the minimum value of each 2x2 block of pixels
      MAX: Take the maximum value of each 2x2 block of pixels
    """

    DECIMATE: ClassVar[
        DownsamplingMethod
    ]  # value = <DownsamplingMethod.DECIMATE: 1>
    MEAN: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.MEAN: 2>
    MIN: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.MIN: 3>
    MAX: ClassVar[DownsamplingMethod]  # value = <DownsamplingMethod.MAX: 4>
    __members__: ClassVar[
        dict[str, DownsamplingMethod]
    ]  # value = {'DECIMATE': <DownsamplingMethod.DECIMATE: 1>, 'MEAN': <DownsamplingMethod.MEAN: 2>, 'MIN': <DownsamplingMethod.MIN: 3>, 'MAX': <DownsamplingMethod.MAX: 4>}

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

class IntermediateGroups:
    """
    Whether the stream writes ``zarr.json`` for the groups above its arrays,
    HCS plate and well groups included. The stream writes these groups when it
    is created, and again only when it is given custom metadata for one of
    them.

    Attributes:
      IF_MISSING: Write a group unless a valid Zarr v3 group node is already
        there. Clear the consolidated metadata of a kept group. With
        ``overwrite=True``, write it always. The default.
      ALWAYS: Always write a group, and replace what is there.
      NEVER: Never write a group. The caller writes the hierarchy.
    """

    IF_MISSING: ClassVar[
        IntermediateGroups
    ]  # value = <IntermediateGroups.IF_MISSING: 0>
    ALWAYS: ClassVar[
        IntermediateGroups
    ]  # value = <IntermediateGroups.ALWAYS: 1>
    NEVER: ClassVar[
        IntermediateGroups
    ]  # value = <IntermediateGroups.NEVER: 2>
    __members__: ClassVar[
        dict[str, IntermediateGroups]
    ]  # value = {'IF_MISSING': <IntermediateGroups.IF_MISSING: 0>, 'ALWAYS': <IntermediateGroups.ALWAYS: 1>, 'NEVER': <IntermediateGroups.NEVER: 2>}

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
    """Settings for connecting to and storing data in S3.

    Attributes:
        bucket_name: Name of the destination bucket, which must already exist.
            Requests use virtual-host addressing for an "*.amazonaws.com"
            endpoint whose bucket name is a legal DNS label, and path-style
            addressing otherwise; a name containing a dot, an underscore or an
            upper-case letter falls back to path style even on AWS.
        endpoint: Endpoint URL, which must begin with "http://" or "https://".
            There is no default scheme.
        region: Optional region. Defaults to "us-east-1"; endpoints that
            validate the signing region require it to be set explicitly to
            match the server.
    """

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
        max_threads: Maximum number of threads for parallel processing. Defaults to 0,
            meaning: use the ZARR_MAX_THREADS environment variable if set, otherwise
            auto-detect based on available hardware concurrency.
        custom_metadata: Custom metadata that the stream writes when it is
            created, in order. See CustomMetadata.
        overwrite: If True, removes any existing data at store_path before writing.
        intermediate_groups: Whether to write ``zarr.json`` for the groups
            above the arrays. Defaults to IntermediateGroups.IF_MISSING.

    Note:
        For S3 storage with endpoint "http://localhost:9000", bucket "my-bucket",
        and store_path "my-dataset.zarr", objects are written under the key prefix
        "my-dataset.zarr/" in "my-bucket", which that endpoint serves at
        "http://localhost:9000/my-bucket/my-dataset.zarr/...". The request URL
        depends on the addressing style; see S3Settings.
    """

    arrays: List[ArraySettings]
    custom_metadata: List[CustomMetadata]
    s3: Optional[S3Settings]
    store_path: str
    max_threads: int
    overwrite: bool
    intermediate_groups: IntermediateGroups

    def __init__(self, **kwargs) -> None: ...
    def __repr__(self) -> str: ...
    def get_maximum_memory_usage(self) -> int:
        """Get the maximum memory usage of the stream in bytes."""

    def get_array_keys(self) -> List[str]:
        """Get the list of array keys configured in this stream."""

    @staticmethod
    def from_file(path: str) -> "StreamSettings":
        """Load stream settings from a YAML or JSON config file."""

    @staticmethod
    def from_string(text: str) -> "StreamSettings":
        """Load stream settings from a YAML or JSON config string."""

    @staticmethod
    def from_dict(data: dict) -> "StreamSettings":
        """Build stream settings from a config dict (same schema as files)."""

    def to_dict(self) -> dict:
        """Serialize stream settings to a config dict."""

    def to_file(self, path: str) -> None:
        """Dump settings to a config file (``.json`` -> JSON, else YAML)."""

    def to_yaml(self) -> str:
        """Serialize stream settings to a YAML string."""

    def to_json(self) -> str:
        """Serialize stream settings to a JSON string."""

class ZarrStream:
    def __init__(self, arg0: StreamSettings) -> None: ...
    def append(self, data: numpy.ndarray, key: str | None = None) -> None:
        """Append data to the array at ``key``.

        Returns once the stream has accepted the data, not once the data is
        written to the store; compression and I/O run on worker threads. A
        write failure raises ``RuntimeError``: from this call if the stream
        stops during the call and this call's data is not all accepted,
        otherwise from a later call to ``append`` or ``skip``, or from
        ``close``.

        A stream expects a single producer. The binding releases the GIL
        while it appends, so do not call ``append`` or ``skip`` on one stream
        from two Python threads at the same time.
        """
    def skip(self, n_bytes: int, key: str | None = None) -> None: ...
    def write_custom_metadata(
        self,
        metadata: str | dict,
        *,
        node_key: str | None = None,
        metadata_key: str | None = None,
    ) -> bool:
        """Write custom metadata to a node of the stream.

        For an array, a level of detail, or a multiscale image group, the
        metadata is written with the array's ``zarr.json``, at the latest when
        the stream is closed. Any other group's ``zarr.json`` is written at
        once, and again at close.

        Args:
            metadata: JSON object, as a dict or a string.
            node_key: Key of the node, relative to the store root: an array,
                a group that the stream writes (the root ``""``, or a group
                between the root and an array, such as an HCS plate or
                well), or a level of detail of a multiscale array
                (``"<array key>/<level>"``).
                If None, the stream's only array.
            metadata_key: Key under ``attributes`` to write to. If None,
                each top-level key of ``metadata`` is written under
                ``attributes``. OME-NGFF metadata goes under ``ome``, as an
                object, and merges with the stream's own ``ome`` keys. A
                later custom ``ome`` replaces an earlier one.
                ``ome.multiscales`` is refused, and so is any other key that
                the stream writes under ``ome`` on the node (``version`` and
                ``name`` on a multiscale group), except an equal ``version``.

        Raises:
            RuntimeError: If the node is not found or not addressable, or the
                metadata is not valid.
        """
    def is_active(self) -> bool: ...
    def close(self) -> None: ...
    def get_current_memory_usage(self) -> int:
        """Get the current memory usage of the stream in bytes."""

class ZarrVersion:
    """
    Zarr format version.

    Attributes:
      V3: Zarr format version 3.
    """

    V3: ClassVar[ZarrVersion]  # value = <ZarrVersion.V3: 3>
    __members__: ClassVar[
        dict[str, ZarrVersion]
    ]  # value = {'V2': <ZarrVersion.V2: 0>, 'V3': <ZarrVersion.V3: 1>}

    def __eq__(self, other: Any) -> bool: ...
    def __getstate__(self) -> int: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...

def get_log_level() -> LogLevel:
    """Get the current log level for the Zarr API"""

def set_log_level(level: LogLevel) -> None:
    """Set the log level for the Zarr API"""
