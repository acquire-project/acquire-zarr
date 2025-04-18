# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.4] - 2025-03-25

### Fixed
- Explicitly assign S3 port when none is specified (#71)

### Changed
- Performance enhancements (#72)

## [0.2.3] - 2025-03-12

### Fixed
- Unwritten data in acquisitions with large file counts (#69)

## [0.2.2] - 2025-02-25

### Added
- Support OME-NGFF 0.5 in Zarr V3 (#68)

## [0.2.1] - 2025-02-25

### Added
- Digital Object Identifier (DOI) (#56)

### Fixed
- Default compression level is now 1 (#66)
- Improve docstrings for mkdocstrings compatibility
- Add crc32c to requirements in README

### Changed
- Chunks are written into per-shard buffers in ZarrV3 writer (#60)

## [0.2.0] - 2025-02-11

### Added
- Region field to S3 settings (#58)

### Fixed
- Wheel packaging to include stubs (#54)
- Buffer overrun on partial frame append (#51)

## [0.1.0] - 2025-01-21

### Added
- API parameter to cap thread usage (#46)
- More examples (and updates to existing ones) (#36)

### Fixed
- Missing header that caused build failure (#40)

### Changed
- Buffers are compressed and flushed in the same job (#43)

## [0.0.5] - 2025-01-09

### Changed
- Use CRC32C checksum rather than CRC32 for chunk indices (#37)
- Zarr V3 writer writes latest spec (#33)

### Fixed
- Memory leak (#34)
- Development instructions in README (#35)

## [0.0.3] - 2024-12-19

### Added
- C++ benchmark for different chunk/shard/compression/storage configurations (#22)

### Changed
- Build wheels for Python 3.9 through 3.13 (#32)
- Remove requirement to link against acquire-logger (#31)

## [0.0.2] - 2024-11-26

### Added
- Manylinux wheel release (#19)

## [0.0.1] - 2024-11-08

### Added
- Initial release wheel
