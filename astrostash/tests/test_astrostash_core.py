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
    queries_columns = sql1.get_columns("queries")
    expected_queries_columns = ['id', 'hash', 'last_refreshed', 'refresh_rate']
    assert queries_columns == expected_queries_columns
    with pytest.raises(ValueError):
        sql1.get_columns("xxx")
    assert sql1._check_table_exists("nicermastr") is False
    assert sql1._check_table_exists("queries") is True
    refresh_rate1 = sql1.get_refresh_rate(id1)
    assert refresh_rate1 == 14
    refresh_rate1 = sql1.get_refresh_rate(2)
    assert refresh_rate1 is None
    qp["catalog"] = "xtemaster"
    qp_hash2 = astrostash.sha256sum(qp)
    sql1.insert_query(qp_hash2, None)
    sql1.close()
    os.remove("astrostash.db")
    sql2 = astrostash.SQLiteDB(db_name="astrostash/tests/astrostash_test.db")
    # Test locate_date operations
    demo_product_path = "/DEMO/PATH/TO/nicermastr/1013010107/"
    dpid = sql2.insert_local_data_path("nicermastr",
                                       43555,
                                       demo_product_path)
    assert dpid == 1
    local_data_frame = sql2.get_local_data_paths_by_catalog("nicermastr")
    dummy_frame = pd.DataFrame({"id": [1],
                                "catalog": ["nicermastr"],
                                "rowid": ["43555"],
                                "location": [demo_product_path]})
    pd.testing.assert_frame_equal(local_data_frame, dummy_frame)
    sql2.close()
    assert pl.Path("astrostash/tests/astrostash_test.db").is_file() is True
    os.remove("astrostash/tests/astrostash_test.db")
