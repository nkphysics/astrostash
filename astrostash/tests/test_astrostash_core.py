import astrostash
import os
import pathlib as pl
from datetime import datetime
import pytest
import pandas as pd
from astropy.table import Table
from astropy.coordinates import SkyCoord
from unittest.mock import MagicMock


def test_sha256sum():
    query_params = {
        "query": "PSR B0531+21",
        "catalog": "xtemaster"
        }
    object_hash = astrostash.sha256sum(query_params)

    region_query_params = {
        "query": SkyCoord.from_name("PSR B0531+21"),
        "catalog": "xtemaster"
        }
    region_hash = astrostash.sha256sum(region_query_params)
    # assert no change to query dtype
    assert isinstance(region_query_params["query"], SkyCoord)
    assert object_hash != region_hash


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


def test_update_last_refreshed(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    query_params = {"query": "PSR B0531+21", "catalog": "xtemaster"}
    query_hash = astrostash.sha256sum(query_params)
    sql.insert_query(query_hash, None)
    today = datetime.today().strftime('%Y-%m-%d')
    assert sql.get_query(query_hash)["last_refreshed"][0] == today
    row_updated = sql.update_last_refreshed(1)
    assert row_updated == 1


def test_update_refresh_rate(setup_sqlite_db):
    sql = setup_sqlite_db[0]
    query_params = {"query": "PSR B0531+21", "catalog": "numaster"}
    query_hash = astrostash.sha256sum(query_params)
    queryid = sql.insert_query(query_hash, 7)
    updateid = sql.update_refresh_rate(1, 8)
    assert queryid == updateid
    query = sql.get_query(query_hash)
    assert query['refresh_rate'][0] == 8
    queryid2 = sql._get_queryid(query, False, refresh_rate=20)[0]
    assert queryid == queryid2


def test_fetch_sync(setup_sqlite_db):
    sql, db_path = setup_sqlite_db

    def run_test(refresh, expected_df):
        mock_func_resp = Table.from_pandas(expected_df)
        mock_query_func = MagicMock(return_value=mock_func_resp)
        query_params = {
            'param1': 'value1',
            'refresh_rate': 7,
            'refresh': refresh
        }
        result_df = sql.fetch_sync(
            mock_query_func,
            'test_table',
            query_params,
            None,
            refresh=refresh
        )

        assert not result_df.empty
        mock_query_func.assert_called_once()
        expected_kwargs = {k: v for k, v in query_params.items()
                           if k not in ['refresh_rate', 'refresh']}
        mock_query_func.assert_called_once_with(**expected_kwargs)
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Special assertion for refresh=False
        if not refresh:
            assert sql._check_table_exists("test_table") is True

    # First test case: refresh=False
    mock_df = pd.DataFrame({'__row': ['1', '2'], 'col1': ['a', 'b']})
    run_test(False, mock_df)

    # Second test case: refresh=True
    mock_df2 = pd.DataFrame({'__row': ['1', '2', '3'],
                             'col1': ['a', 'b', 'c']})
    run_test(True, mock_df2)

    # 3rd test case: refresh=True but the response dataframe has no changes
    run_test(True, mock_df2)

    # 4th test case: returned data table is same size as the existing data,
    # but one value of the new table is different
    mock_df3 = pd.DataFrame({'__row': ['1', '2', '3'],
                             'col1': ['a', 'b', 'd']})
    run_test(True, mock_df3)


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
