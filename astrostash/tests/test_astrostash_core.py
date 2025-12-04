import astrostash
import os
import pathlib as pl
from datetime import datetime
import pytest
import pandas as pd


def test_sha256sum():
    query_params = {
        "query": "PSR B0531+21",
        "catalog": "xtemaster"
        }
    astrostash.sha256sum(query_params)


def test_need_refresh():
    assert astrostash.needs_refresh("2020-01-01", 5) is True
    d2 = datetime.today().strftime('%Y-%m-%d')
    assert astrostash.needs_refresh(d2, 5) is False


@pytest.fixture
def setup_sqlite_db(tmpdir):
    db_path = tmpdir.join("astrostash_test.db")
    sql = astrostash.SQLiteDB(db_name=str(db_path))
    yield sql, db_path
    sql.close()
    os.remove(db_path)
    # Ensure the file is deleted after the test
    assert pl.Path(db_path).is_file() is False


def test_sqlitedb_init(setup_sqlite_db):
    sql, db_path = setup_sqlite_db
    assert pl.Path(db_path).is_file() is True


def test_insert_and_retrieve_query(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    query_params = {"query": "PSR B0531+21", "catalog": "nicermastr"}
    query_hash = astrostash.sha256sum(query_params)
    id1 = sql.insert_query(query_hash, 14)
    assert id1 == 1
    result = sql.get_query(query_hash)
    assert not result.empty
    assert result.hash.iloc[0] == query_hash
    assert result.refresh_rate.iloc[0] == 14


def test_get_query_nonexistent(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    query_params = {"query": "PSR B0531+21", "catalog": "nicermastr"}
    query_hash = astrostash.sha256sum(query_params)
    # Test query that doesn't exist
    result = sql.get_query(query_hash)
    assert result.empty


def test_check_table_columns(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    expected_columns = ['id', 'hash', 'last_refreshed', 'refresh_rate']
    assert sql.get_columns("queries") == expected_columns


def test_invalid_table_columns(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    with pytest.raises(ValueError):
        sql.get_columns("xxx")


def test_check_table_exists(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    assert sql._check_table_exists("queries") is True
    assert sql._check_table_exists("nicermastr") is False


def test_get_refresh_rate(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    query_params = {"query": "PSR B0531+21", "catalog": "nicermastr"}
    query_hash = astrostash.sha256sum(query_params)
    sql.insert_query(query_hash, 14)
    # Test existing ID
    assert sql.get_refresh_rate(1) == 14
    # Test non-existent ID
    assert sql.get_refresh_rate(2) is None


def test_insert_local_data_path(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    demo_product_path = "/DEMO/PATH/TO/nicermastr/1013010107/"
    # Insert local data path
    dpid = sql.insert_local_data_path("nicermastr", 43555, demo_product_path)
    assert dpid == 1
    # Retrieve and assert
    local_data_frame = sql.get_local_data_paths_by_catalog("nicermastr")
    dummy_frame = pd.DataFrame({
        "id": [1],
        "catalog": ["nicermastr"],
        "rowid": ["43555"],
        "location": [demo_product_path]
    })
    pd.testing.assert_frame_equal(local_data_frame, dummy_frame)
