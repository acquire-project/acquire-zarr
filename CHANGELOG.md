# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - [2025-09-24](https://github.com/acquire-project/acquire-zarr/compare/v0.5.2...v0.6.0)

### Added

- New API methods for determining the maximum and current memory usage of a stream (#148)
- Support for high-content screening (HCS) workflows with NGFF 0.5 metadata (#153)
- New API function for retrieving the distinct array keys from a `ZarrStreamSettings` object (#154)

### Fixed

- A bug affecting the Zarr V3 writer that caused it to skip writing the chunk table on the final shard file when the
  shard was not completely full on shutdown (#159)

## [0.5.2] - [2025-08-07](https://github.com/acquire-project/acquire-zarr/compare/v0.5.1...v0.5.2)

### Added

- Examples are now packaged with the library (#143)
- Support for `find_package(acquire-zarr)` in CMake (#143)

### Changed

- Default to streaming Zarr V3 in Python API (#142)

### Fixed

- Race condition and use-after-free bugs during teardown on macOS and Ubuntu (#144)

## [0.5.1] - [2025-07-24](https://github.com/acquire-project/acquire-zarr/compare/v0.5.0...v0.5.1)

### Added

- Users may specify NumPy datatypes when configuring streams (#140)

### Changed

- Linux wheels now support glibc 2.28 and later (#137)

### Fixed

- Endianness indicator for 1-byte dtypes has been corrected to `|` (not relevant) in Zarr V2 metadata (#138)

## [0.5.0] - [2025-07-11](https://github.com/acquire-project/acquire-zarr/compare/v0.4.0...v0.5.0)

### Added

- Users may now select the method used to downsample images (#108)
- Downsampling metadata now includes reproducible method specifications with exact library functions and parameters (
  #118)
- Added `output_key` option to specify the key/path in Zarr storage where data should be saved (#106)
- Added `overwrite` flag to control whether existing data in the store path should be removed (#106)
- Added support for IAM and config file options for S3 authentication (#109)
- A `close()` method has been added to the Python API to ensure all data is flushed and resources are released (#130)
- Users may now stream to multiple output arrays (#128)
- Added support for ARM wheels on Linux (#132)

## Changed

- Downsampling operations are now serializable and reproducible using standard library functions (#118)
- Enhanced test coverage for all downsampling methods with metadata validation (#118)
- Limit OpenMP parallelization to single thread on systems with ≤4 cores to avoid crashes in constrained environments (
  #111)
- Improved thread safety with additional mutex protection for buffer operations (#111)
- Enhanced error handling with more descriptive bounds checking and assertions (#111)
- Decouple XY and Z downsampling for anisotropic volumes (#117)

### Fixed

- Segmentation faults in containerized environments (CI, CoreWeave, Argo) caused by OpenMP threading issues (#111)
- Race conditions in lambda capture and reference handling in thread pool jobs (#111)
- Buffer overflow checks in V3 array defragmentation (#111)
- Spatial downsampling now correctly handles an odd-sized Z dimension (#134)
- Buffers are correctly flushed on Windows before closing the stream (#135)

### Deprecated

- Streaming to Zarr V2 is now deprecated (#110)

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
