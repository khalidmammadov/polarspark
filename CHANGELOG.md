# Changelog

## [0.2.5rc3] - 2026-01-22
### Added
- chore(release): v0.2.5rc2


## [0.2.5rc2] - 2026-01-22
### Added



## [0.2.5rc1] - 2026-01-22
### Added
- Polarspark activation


## [0.2.3rc1] - 2026-01-15
### Added
- Add USING clause when equi join and columns are the same (#87)
- Introduce DuckDB for SQL queiries (#86)
- feat(DataFrame): Port cross join tests for DataFrame operations (#85)
- feat(DataFrame): Ported Scala DataFrame join test (first batch) (#84)


## [0.2.2a4] - 2025-12-18
### Fixed
- Fix Multiple Insert using SQL statement 

## [0.2.1] - 2025-12-16
  
Streaming, In Memory Catalog, DDL executions
 
### Added
- Stateless Streaming 
  - File based streaming (#51) (296142f)
  - Stream stop (#52) (7d5ebef)
  - Streaming await any (#54) (1c0dc60)
  - WIP streaming, spark.read(text) schema on read fix (2263656)
- Add InMemory Catalog
- SQL: Create, Insert, Drop and Select
 
### Changed
- Introduce Transformers and Generators for DataFrame builds
### Fixed
- Fix Rate stream processing and long-running tests and simplify (#56) (bf5dae1)

