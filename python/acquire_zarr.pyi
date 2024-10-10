from typing import (
    Any,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    final,
    overload,
)

class ZarrVersion:
    V2: ClassVar[ZarrVersion]
    V3: ClassVar[ZarrVersion]

class DataType:
    UINT8: ClassVar[DataType]
    UINT16: ClassVar[DataType]
    UINT32: ClassVar[DataType]
    UINT64: ClassVar[DataType]
    INT8: ClassVar[DataType]
    INT16: ClassVar[DataType]
    INT32: ClassVar[DataType]
    INT64: ClassVar[DataType]
    FLOAT32: ClassVar[DataType]
    FLOAT64: ClassVar[DataType]

class Compressor:
    NONE: ClassVar[Compressor]
    BLOSC1: ClassVar[Compressor]

class CompressionCodec:
    NONE: ClassVar[CompressionCodec]
    BLOSC_LZ4: ClassVar[CompressionCodec]
    BLOSC_ZSTD: ClassVar[CompressionCodec]

class DimensionType:
    SPACE: ClassVar[DimensionType]
    CHANNEL: ClassVar[DimensionType]
    TIME: ClassVar[DimensionType]
    OTHER: ClassVar[DimensionType]

class S3Settings:
    endpoint: str
    bucket_name: str
    access_key_id: str
    secret_access_key: str

class CompressionSettings:
    compressor: Compressor
    codec: CompressionCodec
    level: int
    shuffle: int

class Dimension:
    name: str
    kind: DimensionType
    array_size_px: int
    chunk_size_px: int
    shard_size_chunks: int

class StreamSettings:
    store_path: str
    custom_metadata: Optional[str]
    s3: Optional[S3Settings]
    compression: Optional[CompressionSettings]
    dimensions: List[Dimension]
    multiscale: bool
    data_type: DataType
    version: ZarrVersion

class ZarrStream:
    def append(self, data: Any) -> None:
        ...

    def is_active(self) -> bool:
        ...