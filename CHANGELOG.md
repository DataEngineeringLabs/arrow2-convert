## Changelog

All notable changes to `arrow2-convert` project will be documented in this file.

## [Unreleased]
## [0.4.0]
### Changed
- MSRV is bumped to 1.65.0 due to dependency on GATs (#73, @aldanor).
- `ArrowDeserialize` can now be used as a standalone trait bound (#73, @aldanor).

## [0.3.1] - 2022-09-29
### Changed
- Update arrow2 version to 0.14 (#66, @ncpenke).

## [0.3.0] - 2022-08-25
### Added
- Add support for converting to `Chunk` (#44, @ncpenke).
- Add support for `i128` (#48, @ncpenke).
- Add support for enums (#37, @ncpenke).
- Add support for flattening chunks (#56, @nielsmeima).

### Changed
- Serialize escaped Rust identifiers unescaped (#59, @teymour-aldridge).
- Update arrow2 version to 0.13 (#61, @teymour-aldridge).

## [0.2.0] - 2022-06-13
### Added
- Add support for `FixedSizeBinary` and `FixedSizeList` (#30, @ncpenke).

### Changed
- Update arrow2 version to 0.12 (#38, @joshuataylor).

## [0.1.0] - 2022-03-03
Initial crate release.
