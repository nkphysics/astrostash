import astrostash
import os
import pathlib as pl
from datetime import datetime


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


def test_SQLiteDB():
    sql1 = astrostash.SQLiteDB()
    assert pl.Path("astrostash.db").is_file() is True
    # Test getting query that does not exist
    qp = {"query": "PSR B0531+21",
          "catalog": "nicermastr"}
    qp_hash = astrostash.sha256sum(qp)
    assert sql1.get_query(qp_hash).empty
    # Test Insertion
    id1 = sql1.insert_query(qp_hash, 14)
    assert id1 == 1
    # Test getting query that already exists
    assert sql1.get_query(qp_hash).hash[0] == qp_hash
    refresh_rate1 = sql1.get_refresh_rate(id1)
    assert refresh_rate1 == 14
    refresh_rate1 = sql1.get_refresh_rate(2)
    assert refresh_rate1 is None
    qp["catalog"] = "xtemaster"
    qp_hash2 = astrostash.sha256sum(qp)
    id2 = sql1.insert_query(qp_hash2, None)
    sql1.close()
    os.remove("astrostash.db")
    sql2 = astrostash.SQLiteDB(db_name="astrostash/tests/astrostash_test.db")
    sql2.close()
    assert pl.Path("astrostash/tests/astrostash_test.db").is_file() is True
    os.remove("astrostash/tests/astrostash_test.db")
