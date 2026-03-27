import sqlite3
import shutil
import pytest
from astrostash.heasarc import Heasarc

TEST_DB = "astrostash/heasarc/tests/data/processed-conflict.db"

_catalog_failures = []


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if _catalog_failures:
        terminalreporter.section("Catalogs that failed stashing")
        for catalog, error in _catalog_failures:
            terminalreporter.write_line(f"  FAILED: {catalog} -- {error}")


def _get_all_catalogs():
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.execute("SELECT name FROM heasarc_catalog_list")
    catalogs = [row[0] for row in cursor.fetchall()]
    conn.close()
    return catalogs


@pytest.mark.table_schemas
@pytest.mark.parametrize("catalog", _get_all_catalogs())
def test_catalog_has_row_column(catalog, tmp_path):
    db_copy = str(tmp_path / "test.db")
    shutil.copy(TEST_DB, db_copy)
    heasarc = Heasarc(db_copy)
    try:
        result = heasarc.query_tap(
            f"SELECT TOP 1 * FROM {catalog}",
            catalog=catalog,
            refresh=True
        )
        assert "__row" in result.columns, (
            f"Catalog '{catalog}' missing '__row' column"
        )
    except Exception as e:
        _catalog_failures.append((catalog, str(e)))
        pytest.fail(f"Catalog '{catalog}' failed to stash: {e}")
