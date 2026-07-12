# v0.2.3

## Bug Fixes
- Fixed duplicate rows when caching via `query_region` then mirroring the full catalog (or vice versa) via `Heasarc.stash_full_catalog` by using ID-based deduplication (#27)

# v0.2.2

## Improvements
- Added caching support for `SQLiteDB.query_region` in local mode (#26)
- Added indexes to pivot tables for faster cache lookups (#26)

## Refactors
- Reverted from pandas table filtering to an optimized sqlite query in `SQLiteDB._get_stashed_rows` (#26)

# v0.2.1

## Bug Fixes
- Added default radius for cone queries in local mode — fixes ValueError when radius not specified (#22)

## Improvements
- Created `Heasarc.stash_full_catalog` to fetch entire catalogs in chunks to avoid DALOverflowWarnings (#21)

# v0.2.0

## New Features
- Local spatial queries via `SQLiteDB.query_region` (cone, box, polygon, all-sky)
- `mode` kwarg for `Heasarc.query_region` and `Heasarc.query_object` (standard / refresh / local)
- `Heasarc.locate_data()` — find download links for data products
- `Heasarc.download_data()` — download data products and track local paths
- `local_data_paths` table for tracking downloaded data products
- Local mirroring workflow — stash entire tables via `query_tap`, query offline with `mode='local'`

## Improvements
- NumPy-style docstrings across all public methods
- Bulk inserts for response row ID pivots
- Refactored dict operations (`del` → `pop`)

## Documentation
- New `docs/heasarc.md` — architecture, quickstart, local mirroring, API reference
- Overhauled README with Documentation section and links

# v0.1.1

- Removed f-string queries from Heasarc #13
- Refactor `SQLiteDB.fetch sync()` to clean up and abstract out some code in #12
- Included `schema/base.sql` in package data in #11
