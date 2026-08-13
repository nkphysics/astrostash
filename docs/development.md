# Development

## Development Setup

Clone the repository and install it:

```bash
cd astrostash
pip install -e ".[dev]"
```

For PostgreSQL support, include the `postgres` extra:

```bash
pip install -e ".[dev,postgres]"
```

## Running Tests

By default, `pytest` runs only local unit tests (excluding remote, postgres,
and schema validation tests).

To run specific test groups:

```bash
# PostgreSQL integration tests (requires a running PostgreSQL instance)
pytest -m postgres -v

# Remote HEASARC tests (requires network access)
pytest -m remote -v

# Schema validation tests
pytest -m table_schemas -v
```

> **Note:** The `table_schemas` tests only validate which HEASARC catalogs
> are queryable. If this needs to be run, it will only be manually. In reality you will
> never need to run these tests. They were
> created for determining what catalogs (if any) don't work with astrostash.

## PostgreSQL Testing

Start a local PostgreSQL instance using Docker:

```bash
docker-compose up -d
```

This starts PostgreSQL 16 with the following defaults:

| Variable | Value |
|----------|-------|
| `ASTROSTASH_PGHOST` | `127.0.0.1` |
| `ASTROSTASH_PGPORT` | `5432` |
| `ASTROSTASH_PGDATABASE` | `astrostash_test` |
| `ASTROSTASH_PGUSER` | `astrostash` |
| `ASTROSTASH_PGPASSWORD` | `astrostash_test` |

Override any of these by setting the environment variables before running
tests. When done:

```bash
docker-compose down
```

> **Note:** CI uses `astrostash_ci` as the password instead of `astrostash_test`.

## CI Overview

The CI pipeline runs on push to `master` and pull requests to `master`.
It consists of two jobs:

1. **build** : Lints with flake8 and runs local unit tests across
   Python 3.10, 3.11, 3.12, and 3.13.

2. **test-postgres** : Runs after `build` passes. Tests against
   PostgreSQL 16 and 17 across the same Python version matrix.
