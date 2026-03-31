![GitHub License](https://img.shields.io/github/license/nkphysics/astrostash)
![GitHub branch check runs](https://img.shields.io/github/check-runs/nkphysics/astrostash/master)

# astrostash

🔭 **An astronomy and astrophysics database building/syncing tool**

Astrostash lets you "stash" query results from astronomy data sources into a local SQLite3 database by caching individual queries, or mirroring entire catalog tables for offline access.

---

## Features

- **Local data storage** — Retain copies of query results in a SQLite3 database.
- **Offline querying** — Mirror stable catalog tables and query them with spatial filters without network access.
- **Automatic caching** — Results from repeated queries are served from your local database.
- **Refresh scheduling** — Set a refresh interval to keep cached data up to date.

---

## Requirements

Python >= 3.10

### 📦 Dependencies

- `astroquery >= 0.4.10`
- `pandas >= 2.3.0`
- `SQLAlchemy >= 2.0.43`

---

## 📥 Installation

```bash
pip install astrostash
```

Or clone and install from source:

```bash
git clone https://github.com/nkphysics/astrostash.git
cd astrostash
pip install .
```

For info on **Getting Started** see the docs linked below.

---

## 📚 Documentation

- **[HEASARC](docs/heasarc.md)** — Quickstart, local mirroring workflow, spatial queries, data products, and full API reference.

---

## Development

🚧 Current State: Alpha

Install dev dependencies:

```bash
pip install .[dev]
```

---

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
