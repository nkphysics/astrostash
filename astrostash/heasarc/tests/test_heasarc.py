from astrostash.heasarc import Heasarc
from astropy.coordinates import SkyCoord
import os
import pathlib as pl
import shutil
import pytest
import pandas as pd


@pytest.fixture
def cleanup_copies():
    yield
    os.remove("astrostash/heasarc/tests/data/processed-conflict-copy.db")


@pytest.fixture
def setup():
    heasarc = Heasarc("astrostash/heasarc/tests/data/processed-conflict.db")
    yield heasarc


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
    assert len(alias_query) == len(crab_refresh)
    changed_row = crab_refresh.loc[
        crab_refresh["__row"] == "43561"
        ].reset_index(drop=True)
    assert len(changed_row) == 1
    assert changed_row.at[0, "processing_status"] == "VALIDATED"
    # Test pull existing data
    aql_x1 = heasarc2.query_object("AQL X-1", catalog="nicermastr")
    assert len(aql_x1) == 302


def test_query_tap(setup):
    setup.query_tap("SELECT * FROM uhuru4", catalog="uhuru4")
    assert setup.ldb._check_table_exists("uhuru4") is True


def test_locate_data(setup):
    crabdf = setup.query_object("PSR B0531+21", catalog="nicermastr")
    products = setup.locate_data(crabdf, "nicermastr")
    expected_columns = ['rowid', 'access_url', 'sciserver', 'aws',
                        'content_length', 'error_message', 'local_id',
                        'location']
    assert products.columns.to_list() == expected_columns
    assert len(products["location"].dropna()) == 0


def test_download_data():
    db = "astrostash/heasarc/tests/data/processed-conflict.db"
    heasarc = Heasarc(db)
    crabdf = heasarc.query_object("PSR B0531+21", catalog="nicermastr")
    products = heasarc.locate_data(crabdf, "nicermastr")
    sel = products.loc[products["rowid"] == "43555"]
    heasarc.download_data(sel, "nicermastr", host="heasarc", location=".")
    local_paths = heasarc.ldb.get_local_data_paths_by_catalog("nicermastr")
    dummy_frame = pd.DataFrame({
        "id": [1],
        "catalog": ["nicermastr"],
        "rowid": ["43555"],
        "location": [str(pl.Path(f"./1013010107").resolve())]
    })
    pd.testing.assert_frame_equal(local_paths, dummy_frame)
