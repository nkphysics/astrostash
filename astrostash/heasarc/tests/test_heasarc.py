from astrostash.heasarc import Heasarc
from astropy.coordinates import SkyCoord
import os


def test_list_catalogs():
    heasarc = Heasarc()
    cat_list_get = heasarc.list_catalogs()
    assert "nicermastr" in cat_list_get["name"].values
    assert heasarc._check_catalog_exists("xtemaster") is True
    assert heasarc.ldb._check_table_exists("heasarc_catalog_list") is True
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
    ngc_table = heasarc.query_region(position=pos, catalog='numaster')
    assert heasarc.ldb._check_table_exists("numaster") is True
    os.remove("astrostash.db")


def test_query_object():
    heasarc = Heasarc()
    crab_table = heasarc.query_object("crab", catalog="nicermastr")
    assert heasarc.ldb._check_table_exists("nicermastr") is True
    os.remove("astrostash.db")


def test_query_tap():
    heasarc = Heasarc()
    table = heasarc.query_tap("SELECT * FROM uhuru4", catalog="uhuru4")
    assert heasarc.ldb._check_table_exists("uhuru4") is True
    os.remove("astrostash.db")