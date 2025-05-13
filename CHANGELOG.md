# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Users may now select the method used to downsample images (#108)

## [0.4.0] - [2025-04-24](https://github.com/acquire-project/acquire-zarr/compare/v0.3.1...v0.4.0)

### Added

- API supports `unit` (string) and `scale` (double) properties to C `ZarrDimensionProperties` struct and Python
  `DimensionProperties` class (#102)
- Support for optional Zarr V3 `dimension_names` field in array metadata (#102)

### Changed

- Modified OME metadata generation to write unit and scale information (#102)

### Removed

- Remove hardcoded "micrometer" unit values from x and y dimensions (#102)

## [0.3.1] - [2025-04-22](https://github.com/acquire-project/acquire-zarr/compare/v0.3.0...v0.3.1)

### Fixed
- Missing chunk columns when shards are ragged (#99)
- Downsample in 2D if the third dimension has size 1 (#100)

## [0.3.0] - [2025-04-18](https://github.com/acquire-project/acquire-zarr/compare/v0.2.4...v0.3.0)

### Added
- Python benchmark comparing acquire-zarr to TensorStore performance (#80)

### Changed
- Metadata may be set at any point during streaming (#74)
- Hide flush latency with a frame queue (#75)
- Make `StreamSettings.dimensions` behave more like a Python list (#81)
- Require S3 credentials in environment variables (#97)
- Downsampling may be done in 2d or 3d depending on the third dimension (#88)

### Fixed
- Transposed Python arrays can be `append`ed as is (#90)

## [0.2.4] - [2025-03-25](https://github.com/acquire-project/acquire-zarr/compare/v0.2.3...v0.2.4)

### Fixed
- Explicitly assign S3 port when none is specified (#71)

### Changed
- Performance enhancements (#72)

## [0.2.3] - [2025-03-12](https://github.com/acquire-project/acquire-zarr/compare/v0.2.2...v0.2.3)

### Fixed
- Unwritten data in acquisitions with large file counts (#69)

## [0.2.2] - [2025-02-25](https://github.com/acquire-project/acquire-zarr/compare/v0.2.1...v0.2.2)

### Added
- Support OME-NGFF 0.5 in Zarr V3 (#68)

## [0.2.1] - [2025-02-25](https://github.com/acquire-project/acquire-zarr/compare/v0.2.0...v0.2.1)

### Added
- Digital Object Identifier (DOI) (#56)

### Fixed
- Default compression level is now 1 (#66)
- Improve docstrings for mkdocstrings compatibility
- Add crc32c to requirements in README

### Changed
- Chunks are written into per-shard buffers in ZarrV3 writer (#60)

## [0.2.0] - [2025-02-11](https://github.com/acquire-project/acquire-zarr/compare/v0.1.0...v0.2.0)

### Added
- Region field to S3 settings (#58)

### Fixed
- Wheel packaging to include stubs (#54)
- Buffer overrun on partial frame append (#51)

## [0.1.0] - [2025-01-21](https://github.com/acquire-project/acquire-zarr/compare/v0.0.5...v0.1.0)

### Added
- API parameter to cap thread usage (#46)
- More examples (and updates to existing ones) (#36)

### Fixed
- Missing header that caused build failure (#40)

### Changed
- Buffers are compressed and flushed in the same job (#43)

## [0.0.5] - [2025-01-09](https://github.com/acquire-project/acquire-zarr/compare/v0.0.3...v0.0.5)

### Changed
- Use CRC32C checksum rather than CRC32 for chunk indices (#37)
- Zarr V3 writer writes latest spec (#33)

### Fixed
- Memory leak (#34)
- Development instructions in README (#35)

## [0.0.3] - [2024-12-19](https://github.com/acquire-project/acquire-zarr/compare/v0.0.2...v0.0.3)

### Added
- C++ benchmark for different chunk/shard/compression/storage configurations (#22)

### Changed
- Build wheels for Python 3.9 through 3.13 (#32)
- Remove requirement to link against acquire-logger (#31)

## [0.0.2] - [2024-11-26](https://github.com/acquire-project/acquire-zarr/compare/v0.0.1...v0.0.2)

### Added
- Manylinux wheel release (#19)

## [0.0.1] - 2024-11-08

### Added
- Initial release wheel
