import pytest
from astrostash import PostgresDB, BaseDB


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
