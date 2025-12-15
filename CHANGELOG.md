## [0.1.11rc1] - 2025-12-15
  
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
