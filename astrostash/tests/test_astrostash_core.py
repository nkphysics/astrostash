import astrostash
import os
import pathlib as pl
from datetime import datetime, date
import pytest
import pandas as pd
from astropy.table import Table
from astropy.coordinates import SkyCoord
from astropy import units as u
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
    assert astrostash.needs_refresh(date(2020, 1, 1), 5) is True
    assert astrostash.needs_refresh(date.today(), 5) is False


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


def test_clear_aq_cache_on_refresh(setup_sqlite_db):
    sql, db_path = setup_sqlite_db

    mock_aq_instance = MagicMock()
    mock_query_func = MagicMock()
    mock_query_func.__self__ = mock_aq_instance
    mock_query_func.return_value = Table.from_pandas(
        pd.DataFrame({'__row': ['1'], 'col1': ['a']})
    )

    query_params = {'param1': 'value1', 'refresh_rate': 7, 'refresh': True}
    sql.fetch_sync(mock_query_func, 'test_table', query_params, None,
                   refresh=True)

    mock_aq_instance.clear_cache.assert_called_once()


def test_no_clear_cache_without_refresh(setup_sqlite_db):
    sql, db_path = setup_sqlite_db

    mock_aq_instance = MagicMock()
    mock_query_func = MagicMock()
    mock_query_func.__self__ = mock_aq_instance
    mock_query_func.return_value = Table.from_pandas(
        pd.DataFrame({'__row': ['1'], 'col1': ['a']})
    )

    query_params = {'param1': 'value1', 'refresh_rate': 7, 'refresh': False}
    sql.fetch_sync(mock_query_func, 'test_table', query_params, None,
                   refresh=False)

    mock_aq_instance.clear_cache.assert_not_called()


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


@pytest.fixture
def setup_spatial_db(setup_sqlite_db):
    sql, db_path = setup_sqlite_db
    df = pd.DataFrame({
        'ra': [10.0, 10.1, 10.5, 20.0, 150.0],
        'dec': [20.0, 20.1, 20.5, 30.0, -45.0],
        'name': ['a', 'b', 'c', 'd', 'e'],
        '__row': ['1', '2', '3', '4', '5']
    })
    sql.ingest_table(df, 'test_cat')
    return sql


def test_query_region_cone(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='cone', radius='0.6deg')
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


def test_query_region_cone_with_quantity(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='cone', radius=0.6 * u.deg)
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


def test_query_region_cone_string_position(setup_spatial_db):
    sql = setup_spatial_db
    result = sql.query_region('test_cat', position='10d 20d',
                              spatial='cone', radius='0.6deg')
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


def test_query_region_box(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='box', width='0.3deg')
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


def test_query_region_box_with_quantity(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='box', width=0.3 * u.deg)
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


def test_query_region_box_ra_wrapping(setup_sqlite_db):
    sql, db_path = setup_sqlite_db
    df = pd.DataFrame({
        'ra': [359.0, 0.0, 180.0, 1.0],
        'dec': [20.0, 20.0, 20.0, 20.0],
        'name': ['a', 'b', 'c', 'd'],
        '__row': ['1', '2', '3', '4']
    })
    sql.ingest_table(df, 'wrap_cat')

    center = SkyCoord(ra=0.0, dec=20.0, unit='deg')
    result = sql.query_region('wrap_cat', position=center,
                              spatial='box', width='4deg')
    assert len(result) == 3
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b', 'd']


def test_query_region_polygon(setup_spatial_db):
    sql = setup_spatial_db
    verts = [(9.8, 19.8), (10.3, 19.8), (10.3, 20.3), (9.8, 20.3)]
    result = sql.query_region('test_cat', spatial='polygon', polygon=verts)
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


def test_query_region_allsky(setup_spatial_db):
    sql = setup_spatial_db
    result = sql.query_region('test_cat', spatial='all-sky')
    assert len(result) == 5


def test_query_region_empty_table(setup_sqlite_db):
    sql, db_path = setup_sqlite_db
    df = pd.DataFrame({
        'ra': pd.Series(dtype='float64'),
        'dec': pd.Series(dtype='float64'),
        'name': pd.Series(dtype='object'),
        '__row': pd.Series(dtype='object')
    })
    sql.ingest_table(df, 'empty_cat')
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('empty_cat', position=center,
                              spatial='cone', radius='1deg')
    assert result.empty


def test_query_region_no_table(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="does not exist"):
        sql.query_region('nonexistent', spatial='all-sky')


def test_query_region_no_ra_dec(setup_sqlite_db):
    sql, db_path = setup_sqlite_db
    df = pd.DataFrame({'x': [1, 2], 'y': [3, 4]})
    sql.ingest_table(df, 'no_radec')
    with pytest.raises(ValueError, match="has no 'ra' and 'dec' columns"):
        sql.query_region('no_radec', spatial='all-sky')


def test_query_region_missing_position(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="position is required"):
        sql.query_region('test_cat', spatial='cone', radius='1deg')


def test_query_region_missing_radius(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    with pytest.raises(ValueError, match="radius is required"):
        sql.query_region('test_cat', position=center, spatial='cone')


def test_query_region_missing_width(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    with pytest.raises(ValueError, match="width is required"):
        sql.query_region('test_cat', position=center, spatial='box')


def test_query_region_missing_polygon(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="polygon is required"):
        sql.query_region('test_cat', spatial='polygon')


def test_query_region_unknown_spatial(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="Unknown spatial mode"):
        sql.query_region('test_cat', spatial='invalid')
