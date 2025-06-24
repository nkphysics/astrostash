import pytest
from astrostash import SQLiteDB
import os
import pathlib as pl


def test_SQLiteDB():
    sql1 = SQLiteDB()
    sql1.close()
    assert pl.Path("astrostash.db").is_file() is True
    os.remove("astrostash.db")
    sql2 = SQLiteDB(db_name="astrostash/tests/astrostash_test.db")
    assert pl.Path("astrostash/tests/astrostash_test.db").is_file() is True
    os.remove("astrostash/tests/astrostash_test.db")