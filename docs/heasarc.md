# astrostash.heasarc

## Quickstart

```python
from astrostash.heasarc import Heasarc
from astropy.coordinates import SkyCoord

h = Heasarc()

# List available catalogs (cached after first call)
catalogs = h.list_catalogs()
print(catalogs[catalogs["name"].str.contains("nicer")])

# Query a catalog by region
pos = SkyCoord.from_name("ngc 3783")
results = h.query_region(position=pos, catalog="nicermastr",
                         radius="0.5deg", refresh_rate=30)
print(results)

# Query by object name (resolves coordinates via CDS/Sesame)
crab = h.query_object("crab", catalog="nicermastr")
```

On the first call, results are fetched from the HEASARC and stored locally.
Subsequent calls with the same parameters return the cached results. If a
`refresh_rate` (in days) is set, results are re-fetched automatically when
the cache expires.

---

## Architecture & Concepts

### Caching Model

astrostash uses a two-layer caching scheme:

1. **Query tracking** — Each unique set of query parameters is hashed with
   SHA-256. The hash is stored in the `queries` table along with the date it
   was last executed and an optional refresh rate.

2. **Response storage** — Query results are stored as named data tables in the
   same SQLite database (e.g., a table called `nicermastr`). The `responses`
   table tracks the hash of each result set, and pivot tables link queries to
   their responses and to individual row IDs.

This means:
- Identical queries (same parameters) reuse cached results automatically.
- Different queries against the same catalog share a single data table,
  with pivot tables tracking which rows belong to which query.
- The database is self-contained — it can be copied, backed up, or shared
  as a single `.db` file.

### Query Modes

The `query_region` and `query_object` methods accept a `mode` parameter:

| Mode | Behavior |
|------|----------|
| `'standard'` (default) | Check local cache first. If no cached data exists (or if the refresh interval has elapsed), fetch from the HEASARC and update the cache. |
| `'refresh'` | Always fetch from the HEASARC, regardless of cache state. Updates the cache with fresh results. |
| `'local'` | Query the local database only. No remote requests are made. Supports spatial filtering (see below). |

### Local Mirroring

Beyond caching repeated queries, astrostash can be used to build a complete
local mirror of a HEASARC catalog. This is useful for stable, historical or legacy
tables (e.g., `uhuru4`, `ariel5`) that will (very likely) not change.
Once stashed, they can be queried entirely offline.

The workflow is two steps:

1. **Stash the table** — Pull the entire catalog into your local database
   using `query_tap`. This is a one-time operation for stable tables.

2. **Query locally** — Use `query_region` or `query_object` with
   `mode='local'` to search the stashed data with spatial filters. No
   network connection is needed.

```python
from astrostash.heasarc import Heasarc
from astropy.coordinates import SkyCoord

h = Heasarc("my_data.db")

# Step 1: Stash the entire table (one-time, requires network)
h.query_tap("SELECT * FROM uhuru4", catalog="uhuru4")

# Step 2: Query locally — no network needed
pos = SkyCoord(ra=10.0, dec=20.0, unit="deg")
results = h.query_region(position=pos, catalog="uhuru4",
                         radius="1deg", mode="local")
print(len(results))

# All spatial types work in local mode
all_sky = h.query_region(catalog="uhuru4", spatial="all-sky", mode="local")
```

The key difference from caching:

- **Caching** (`mode='standard'`) — Stores the results of a specific query
  so the same query doesn't need to hit HEASARC again. The data is tied to
  that query's parameters.
- **Mirroring** (`query_tap` + `mode='local'`) — Stores the entire table.
  You can then run any spatial query against it locally, without any
  dependence on the HEASARC service.

### Spatial Queries (Local Mode)

In `'local'` mode, `query_region` filters catalog data by spatial region.
The table must have `ra` and `dec` columns. Supported spatial types:

| Spatial | Required Parameters | Description |
|---------|--------------------|-------------|
| `'cone'` | `position`, `radius` | Circular search around a center point |
| `'box'` | `position`, `width` | Square box centered on a position |
| `'polygon'` | `polygon` | Arbitrary polygon defined by `(ra, dec)` pairs in decimal degrees |
| `'all-sky'` | *(none)* | Returns all cached rows |

Example — local cone query:

```python
from astropy.coordinates import SkyCoord

h = Heasarc("my_data.db")
pos = SkyCoord(ra=83.633, dec=22.015, unit="deg")
results = h.query_region(position=pos, catalog="nicermastr",
                         radius="0.5deg", mode="local")
```

### Data Products

After querying a catalog, you can locate and download associated data
products:

```python
# Locate available data products for query results
crab = h.query_object("crab", catalog="nicermastr")
products = h.locate_data(crab, "nicermastr")
print(products[["rowid", "aws"]])

# Download from the fastest host (AWS by default)
to_download = products[products["rowid"] == "43555"]
h.download_data(to_download, "nicermastr", location="./data")
```

`locate_data` returns a DataFrame with download links (AWS, SciServer,
HEASARC) and any previously recorded local paths. `download_data` fetches
files and records their local paths in the database for future reference.

---

## API Reference

### `Heasarc`

```python
Heasarc(db_name=None)
```

Create a HEASARC client backed by a local SQLite database.

| Parameter | Type | Description |
|-----------|------|-------------|
| `db_name` | `str` or `None` | Path to the SQLite database file. Defaults to `astrostash.db` in the current directory. |

---

#### `list_catalogs`

```python
list_catalogs(*, master=False, keywords=None,
              refresh_rate=None, refresh=False) -> pd.DataFrame
```

Get all available HEASARC catalogs.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `master` | `bool` | `False` | Return only master catalogs |
| `keywords` | `str` or `list[str]` | `None` | Filter catalogs by keyword(s). Strings are space-AND'ed, lists are OR'ed |
| `refresh_rate` | `int` or `None` | `None` | Cache refresh interval in days |
| `refresh` | `bool` | `False` | Force remote fetch |

**Returns:** `pd.DataFrame` with columns `name` and `description`.

---

#### `query_region`

```python
query_region(position=None, catalog=None, radius=None,
             refresh_rate=None, mode='standard', spatial='cone',
             width=None, polygon=None, **kwargs) -> pd.DataFrame
```

Query a HEASARC catalog for records around a specific region.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `position` | `str` or `SkyCoord` | `None` | Center position. Required for `cone`/`box`. Ignored for `polygon`/`all-sky` |
| `catalog` | `str` | `None` | Catalog name as listed at the HEASARC |
| `radius` | `str` or `Quantity` | `None` | Search radius for cone queries |
| `refresh_rate` | `int` or `None` | `None` | Cache refresh interval in days |
| `mode` | `str` | `'standard'` | `'standard'`, `'refresh'`, or `'local'` |
| `spatial` | `str` | `'cone'` | Local mode spatial type: `'cone'`, `'box'`, `'polygon'`, or `'all-sky'` |
| `width` | `str` or `Quantity` | `None` | Width for box queries (local mode) |
| `polygon` | `list[tuple]` | `None` | `(ra, dec)` pairs in degrees for polygon queries (local mode) |
| `**kwargs` | | | Passed to the underlying HEASARC query method |

**Returns:** `pd.DataFrame` of catalog records.

---

#### `query_object`

```python
query_object(object_name, catalog=None, radius=None,
             refresh_rate=None, mode='standard', spatial='cone',
             width=None, polygon=None, **kwargs) -> pd.DataFrame
```

Query a catalog by object name. Resolves coordinates via
`SkyCoord.from_name`, then delegates to `query_region`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `object_name` | `str` | | Object name (e.g. `'PSR B0531+21'`) |
| `catalog` | `str` | `None` | Catalog name |
| `radius` | `str` or `Quantity` | `None` | Search radius |
| `refresh_rate` | `int` or `None` | `None` | Cache refresh interval in days |
| `mode` | `str` | `'standard'` | `'standard'`, `'refresh'`, or `'local'` |
| `spatial` | `str` | `'cone'` | Local mode spatial type |
| `width` | `str` or `Quantity` | `None` | Width for box queries (local mode) |
| `polygon` | `list[tuple]` | `None` | Polygon vertices (local mode) |
| `**kwargs` | | | Passed to `query_region` |

**Returns:** `pd.DataFrame` of catalog records.

---

#### `query_tap`

```python
query_tap(query, catalog, maxrec=None,
          refresh_rate=None, refresh=False) -> pd.DataFrame
```

Execute an ADQL query against the HEASARC Xamin TAP service.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | `str` | | ADQL query string |
| `catalog` | `str` | | Catalog table name to stash results to |
| `maxrec` | `int` or `None` | `None` | Maximum number of records to return |
| `refresh_rate` | `int` or `None` | `None` | Cache refresh interval in days |
| `refresh` | `bool` | `False` | Force remote fetch |

**Returns:** `pd.DataFrame` of query results.

---

#### `locate_data`

```python
locate_data(result_table, catalog) -> pd.DataFrame
```

Find download links and local paths for data products associated with
query results.

| Parameter | Type | Description |
|-----------|------|-------------|
| `result_table` | `pd.DataFrame` | Results from a previous `query_region`, `query_object`, or `query_tap` |
| `catalog` | `str` | Catalog name |

**Returns:** `pd.DataFrame` with download links (AWS, SciServer, HEASARC)
and any previously recorded local paths.

---

#### `download_data`

```python
download_data(links, catalog, *, host="aws", location=".")
```

Download data products and record their local paths in the database.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `links` | `pd.DataFrame` | | DataFrame with download links (from `locate_data`) |
| `catalog` | `str` | | Catalog the links belong to |
| `host` | `str` | `'aws'` | Download host: `'aws'`, `'sciserver'`, or `'heasarc'` |
| `location` | `str` | `'.'` | Local directory to save files to |

---

## Limitations

- **Alpha status** — API may change between versions.
- **Remote tests** — Tests marked `@pytest.mark.remote` require network
  access to HEASARC. By default, `pytest` runs only local/offline tests.
- **`spatial` parameter** — The `spatial`, `width`, and `polygon` parameters
  are only used when `mode='local'`. In `'standard'` and `'refresh'` modes,
  spatial filtering is delegated to the HEASARC service via
  the underlying HEASARC client.
