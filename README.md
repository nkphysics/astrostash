![GitHub License](https://img.shields.io/github/license/nkphysics/astrostash)
![GitHub branch check runs](https://img.shields.io/github/check-runs/nkphysics/astrostash/master)


# astrostash

**An astronomy and astrophysics database building/syncing tool**

Astrostash is designed to allow users to "stash" query results from astronomy/astrophysics data sources (e.g., HEASARC) into a local SQLite3 database. This ensures data retention and reduces reliance on external services for recurring queries.

---

## 🌟 Features

- **Local data storage**: Retain copies of query results in a SQLite3 database so you can keep your own copy of the data you use and care about having access to.
- **Efficient querying**: Prioritizes querying from your local database before pulling data externally to limit external requests.

---
## Requirements

python >= 3.10

### 📦 Dependencies

- sqlite3
- `astroquery >= 0.4.10`
- `pandas >= 2.3.0`
- `SQLAlchemy >= 2.0.43`

---

## 🚧 Current State

**Version**: `v0.1.1`
**Status**: Alpha

**Supported Archives**: 

- HEASARC 

---

## 📥 Installation

1. Clone the repo

2. Run `python -m pip install .`

OR

Run `pip install astrostash`
