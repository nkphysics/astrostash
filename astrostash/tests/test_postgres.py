import os
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import text

from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table

from astrostash import BaseDB, PostgresDB, sha256sum


@pytest.fixture
def mock_pg_db(mocker):
    mock_engine = mocker.patch('astrostash.astrostash.create_engine')
    mock_inspect = mocker.patch('astrostash.astrostash.inspect')
    mocker.patch('astrostash.astrostash.MetaData')
    mock_files = mocker.patch('astrostash.astrostash.files')

    mock_conn = mocker.MagicMock()
    mock_engine.return_value.connect.return_value = mock_conn

    mock_inspector = mocker.MagicMock()
    mock_inspector.has_table.return_value = True
    mock_inspector.get_columns.return_value = [
        {'name': 'id'}, {'name': 'hash'},
        {'name': 'last_refreshed'}, {'name': 'refresh_rate'}
    ]
    mock_inspect.return_value = mock_inspector

    mock_sql = mock_files.return_value.joinpath.return_value
    mock_sql.read_text.return_value = ""

    db = PostgresDB(
        host="fake_host",
        port=5432,
        dbname="fake_db",
        user="fake_user",
        password="fake_pass"
    )
    yield db, mock_conn, mock_engine


def test_postgresdb_inherits_basedb():
    assert issubclass(PostgresDB, BaseDB)


def test_postgresdb_dialect_insert(mocker):
    mocker.patch('astrostash.astrostash.create_engine')
    mocker.patch('astrostash.astrostash.inspect')
    mocker.patch('astrostash.astrostash.MetaData')
    mocker.patch('astrostash.astrostash.files')
    db = PostgresDB("host", 5432, "db", "user", "pass")
    from sqlalchemy.dialects.postgresql import insert
    assert db._dialect_insert == insert


def test_postgresdb_init_creates_tables(mock_pg_db):
    db, mock_conn, mock_engine = mock_pg_db
    assert db.db_name == "fake_db"
    mock_conn.commit.assert_called()


def test_postgresdb_check_table_exists(mock_pg_db):
    db, mock_conn, _ = mock_pg_db
    assert db._check_table_exists("queries") is True


def test_postgresdb_get_columns(mock_pg_db):
    db, mock_conn, _ = mock_pg_db
    cols = db.get_columns("queries")
    assert cols == ['id', 'hash', 'last_refreshed', 'refresh_rate']


def test_postgresdb_close(mock_pg_db):
    db, mock_conn, mock_engine = mock_pg_db
    db.close()
    mock_conn.close.assert_called_once()
    mock_engine.return_value.dispose.assert_called_once()

# Tests that need an actual postgres instance


@pytest.fixture
def pg_db():
    db = PostgresDB(
        host=os.environ.get("ASTROSTASH_PGHOST", "127.0.0.1"),
        port=int(os.environ.get("ASTROSTASH_PGPORT", "5432")),
        dbname=os.environ.get("ASTROSTASH_PGDATABASE", "astrostash_test"),
        user=os.environ.get("ASTROSTASH_PGUSER", "astrostash"),
        password=os.environ.get("ASTROSTASH_PGPASSWORD", "astrostash_test")
    )
    yield db
    db.sconn.execute(text(
        "TRUNCATE queries, responses, query_response_pivot, "
        "response_rowid_pivot, local_data_paths RESTART IDENTITY CASCADE"
    ))
    db.sconn.commit()
    db.close()


@pytest.fixture
def setup_spatial_db(pg_db):
    df = pd.DataFrame({
        'ra': [10.0, 10.1, 10.5, 20.0, 150.0],
        'dec': [20.0, 20.1, 20.5, 30.0, -45.0],
        'name': ['a', 'b', 'c', 'd', 'e'],
        '__row': ['1', '2', '3', '4', '5']
    })
    pg_db.sconn.execute(text("DROP TABLE IF EXISTS test_cat"))
    pg_db.sconn.commit()
    pg_db.ingest_table(df, 'test_cat')
    yield pg_db
    pg_db.sconn.execute(text("DROP TABLE IF EXISTS test_cat"))
    pg_db.sconn.commit()


@pytest.mark.postgres
def test_postgres_init(pg_db):
    assert pg_db._check_table_exists("queries") is True
    assert pg_db._check_table_exists("responses") is True
    assert pg_db._check_table_exists("query_response_pivot") is True
    assert pg_db._check_table_exists("response_rowid_pivot") is True
    assert pg_db._check_table_exists("local_data_paths") is True


@pytest.mark.postgres
def test_insert_and_retrieve_query(pg_db):
    query_params = {"query": "PSR B0531+21", "catalog": "nicermastr"}
    query_hash = sha256sum(query_params)
    id1 = pg_db.insert_query(query_hash, 14)
    assert id1 == 1
    result = pg_db.get_query(query_hash)
    assert not result.empty
    assert result.hash.iloc[0] == query_hash
    assert result.refresh_rate.iloc[0] == 14


@pytest.mark.postgres
def test_get_query_nonexistent(pg_db):
    query_params = {"query": "PSR B0531+21", "catalog": "nicermastr"}
    query_hash = sha256sum(query_params)
    result = pg_db.get_query(query_hash)
    assert result.empty


@pytest.mark.postgres
def test_check_table_columns(pg_db):
    expected_columns = ['id', 'hash', 'last_refreshed', 'refresh_rate']
    assert pg_db.get_columns("queries") == expected_columns


@pytest.mark.postgres
def test_invalid_table_columns(pg_db):
    with pytest.raises(ValueError):
        pg_db.get_columns("xxx")


@pytest.mark.postgres
def test_check_table_exists(pg_db):
    assert pg_db._check_table_exists("queries") is True
    assert pg_db._check_table_exists("nicermastr") is False


@pytest.mark.postgres
def test_get_refresh_rate(pg_db):
    query_params = {"query": "PSR B0531+21", "catalog": "nicermastr"}
    query_hash = sha256sum(query_params)
    pg_db.insert_query(query_hash, 14)
    assert pg_db.get_refresh_rate(1) == 14
    assert pg_db.get_refresh_rate(2) is None


@pytest.mark.postgres
def test_update_last_refreshed(pg_db):
    query_params = {"query": "PSR B0531+21", "catalog": "xtemaster"}
    query_hash = sha256sum(query_params)
    pg_db.insert_query(query_hash, None)
    today = datetime.today().date()
    result_date = pg_db.get_query(query_hash)["last_refreshed"][0]
    assert result_date == today
    row_updated = pg_db.update_last_refreshed(1)
    assert row_updated == 1


@pytest.mark.postgres
def test_update_refresh_rate(pg_db):
    query_params = {"query": "PSR B0531+21", "catalog": "numaster"}
    query_hash = sha256sum(query_params)
    queryid = pg_db.insert_query(query_hash, 7)
    updateid = pg_db.update_refresh_rate(1, 8)
    assert queryid == updateid
    query = pg_db.get_query(query_hash)
    assert query['refresh_rate'][0] == 8
    queryid2 = pg_db._get_queryid(query, False, refresh_rate=20)[0]
    assert queryid == queryid2


@pytest.mark.postgres
def test_fetch_sync(pg_db):
    def run_test(refresh, expected_df):
        mock_func_resp = Table.from_pandas(expected_df)
        mock_query_func = MagicMock(return_value=mock_func_resp)
        query_params = {
            'param1': 'value1',
            'refresh_rate': 7,
            'refresh': refresh
        }
        result_df = pg_db.fetch_sync(
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

        if not refresh:
            assert pg_db._check_table_exists("test_table") is True

    mock_df = pd.DataFrame({'__row': ['1', '2'], 'col1': ['a', 'b']})
    run_test(False, mock_df)

    mock_df2 = pd.DataFrame({'__row': ['1', '2', '3'],
                             'col1': ['a', 'b', 'c']})
    run_test(True, mock_df2)

    run_test(True, mock_df2)

    mock_df3 = pd.DataFrame({'__row': ['1', '2', '3'],
                             'col1': ['a', 'b', 'd']})
    run_test(True, mock_df3)


@pytest.mark.postgres
def test_insert_local_data_path(pg_db):
    demo_product_path = "/DEMO/PATH/TO/nicermastr/1013010107/"
    dpid = pg_db.insert_local_data_path("nicermastr", 43555, demo_product_path)
    assert dpid == 1
    local_data_frame = pg_db.get_local_data_paths_by_catalog("nicermastr")
    dummy_frame = pd.DataFrame({
        "id": [1],
        "catalog": ["nicermastr"],
        "rowid": ["43555"],
        "location": [demo_product_path]
    })
    pd.testing.assert_frame_equal(local_data_frame, dummy_frame)


@pytest.mark.postgres
def test_query_region_cone(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='cone', radius='0.6deg')
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


@pytest.mark.postgres
def test_query_region_cone_with_quantity(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='cone', radius=0.6 * u.deg)
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


@pytest.mark.postgres
def test_query_region_cone_string_position(setup_spatial_db):
    sql = setup_spatial_db
    result = sql.query_region('test_cat', position='10d 20d',
                              spatial='cone', radius='0.6deg')
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


@pytest.mark.postgres
def test_query_region_box(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='box', width='0.3deg')
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


@pytest.mark.postgres
def test_query_region_box_with_quantity(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = sql.query_region('test_cat', position=center,
                              spatial='box', width=0.3 * u.deg)
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


@pytest.mark.postgres
def test_query_region_polygon(setup_spatial_db):
    sql = setup_spatial_db
    verts = [(9.8, 19.8), (10.3, 19.8), (10.3, 20.3), (9.8, 20.3)]
    result = sql.query_region('test_cat', spatial='polygon', polygon=verts)
    assert len(result) == 2
    names = sorted(result['name'].tolist())
    assert names == ['a', 'b']


@pytest.mark.postgres
def test_query_region_allsky(setup_spatial_db):
    sql = setup_spatial_db
    result = sql.query_region('test_cat', spatial='all-sky')
    assert len(result) == 5


@pytest.mark.postgres
def test_query_region_empty_table(pg_db):
    df = pd.DataFrame({
        'ra': pd.Series(dtype='float64'),
        'dec': pd.Series(dtype='float64'),
        'name': pd.Series(dtype='object'),
        '__row': pd.Series(dtype='object')
    })
    pg_db.ingest_table(df, 'empty_cat')
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    result = pg_db.query_region('empty_cat', position=center,
                                spatial='cone', radius='1deg')
    assert result.empty


@pytest.mark.postgres
def test_query_region_no_table(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="does not exist"):
        sql.query_region('nonexistent', spatial='all-sky')


@pytest.mark.postgres
def test_query_region_no_ra_dec(pg_db):
    df = pd.DataFrame({'x': [1, 2], 'y': [3, 4]})
    pg_db.ingest_table(df, 'no_radec')
    with pytest.raises(ValueError, match="has no 'ra' and 'dec' columns"):
        pg_db.query_region('no_radec', spatial='all-sky')


@pytest.mark.postgres
def test_query_region_missing_position(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="position is required"):
        sql.query_region('test_cat', spatial='cone', radius='1deg')


@pytest.mark.postgres
def test_query_region_missing_radius(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    with pytest.raises(ValueError, match="radius is required"):
        sql.query_region('test_cat', position=center, spatial='cone')


@pytest.mark.postgres
def test_query_region_missing_width(setup_spatial_db):
    sql = setup_spatial_db
    center = SkyCoord(ra=10.0, dec=20.0, unit='deg')
    with pytest.raises(ValueError, match="width is required"):
        sql.query_region('test_cat', position=center, spatial='box')


@pytest.mark.postgres
def test_query_region_missing_polygon(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="polygon is required"):
        sql.query_region('test_cat', spatial='polygon')


@pytest.mark.postgres
def test_query_region_unknown_spatial(setup_spatial_db):
    sql = setup_spatial_db
    with pytest.raises(ValueError, match="Unknown spatial mode"):
        sql.query_region('test_cat', spatial='invalid')
