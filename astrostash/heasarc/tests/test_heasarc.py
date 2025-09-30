from astrostash.heasarc import Heasarc
from astropy.coordinates import SkyCoord
import os
import shutil
import pytest
import pandas as pd


@pytest.fixture
def cleanup_copies():
    yield
    os.remove("astrostash/heasarc/tests/data/processed-conflict-copy.db")


def test_list_catalogs():
    heasarc = Heasarc()
    cat_list_get = heasarc.list_catalogs()
    assert "nicermastr" in cat_list_get["name"].values
    assert heasarc._check_catalog_exists("xtemaster") is True
    assert heasarc.ldb._check_table_exists("heasarc_catalog_list") is True
    assert heasarc.ldb._check_query_response_link(1, 1) != 0
    # Next pull from stashed heasarc_catalog_list table
    just1 = heasarc.list_catalogs(keywords="xte", master=True)
    assert len(just1) == 1
    mrefresh = heasarc.list_catalogs(
        keywords="xte",
        master=True,
        refresh=True)
    assert just1.equals(mrefresh) is True
    cat_list_stash = heasarc.list_catalogs()
    assert cat_list_get.equals(cat_list_stash) is True
    os.remove("astrostash.db")


def test_query_region():
    heasarc = Heasarc()
    pos = SkyCoord.from_name('ngc 3783')
    ngc_table1 = heasarc.query_region(position=pos, catalog='numaster')
    assert heasarc.ldb._check_table_exists("numaster") is True
    ngc_table2 = heasarc.query_region(
        position=pos,
        catalog='numaster',
        refresh_rate=30)
    assert heasarc.ldb.get_refresh_rate(2) == 30
    pd.testing.assert_frame_equal(ngc_table1, ngc_table2)
    os.remove("astrostash.db")


def test_query_object(cleanup_copies):
    heasarc = Heasarc()
    init_query = heasarc.query_object("crab", catalog="nicermastr")
    assert heasarc.ldb._check_table_exists("nicermastr") is True
    alias_query = heasarc.query_object("PSR B0531+21", catalog="nicermastr")
    pd.testing.assert_frame_equal(init_query, alias_query)
    os.remove("astrostash.db")
    dbroot = "astrostash/heasarc/tests/data"
    db = f"{dbroot}/processed-conflict.db"
    dbcopy = f"{dbroot}/processed-conflict-copy.db"
    shutil.copy(db, dbcopy)
    heasarc2 = Heasarc(db_name=dbcopy)
    crab_refresh = heasarc2.query_object(
        "crab", catalog="nicermastr", refresh_rate=2
        )
    changed_row = crab_refresh.loc[crab_refresh["__row"] == "43561"]
    assert len(changed_row) == 1
    assert changed_row.at[187, "processing_status"] == "VALIDATED"
    # Test pull existing data
    aql_x1 = heasarc2.query_object("AQL X-1", catalog="nicermastr")
    assert len(aql_x1) == 302
    # Test pulling new data
    heasarc2.query_object("geminga", catalog="nicermastr")


def test_query_tap():
    heasarc = Heasarc()
    heasarc.query_tap("SELECT * FROM uhuru4", catalog="uhuru4")
    assert heasarc.ldb._check_table_exists("uhuru4") is True
    os.remove("astrostash.db")
