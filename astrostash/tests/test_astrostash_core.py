import pytest
import astrostash
import os
import pathlib as pl


def test_sha256sum():
    query_params = {
        "query": "PSR B0531+21",
        "catalog": "xtemaster"
        }
    astrostash.sha256sum(query_params)


def test_SQLiteDB():
    sql1 = astrostash.SQLiteDB()
    sql1.close()
    assert pl.Path("astrostash.db").is_file() is True
    os.remove("astrostash.db")
    sql2 = astrostash.SQLiteDB(db_name="astrostash/tests/astrostash_test.db")
    sql2.close()
    assert pl.Path("astrostash/tests/astrostash_test.db").is_file() is True
    os.remove("astrostash/tests/astrostash_test.db")
