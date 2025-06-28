from astrostash.heasarc import Heasarc
import os


def test_list_catalogs():
    heasarc = Heasarc()
    cat_list_get = heasarc.list_catalogs()
    assert "nicermastr" in cat_list_get["name"].values
    cat_list_stash = heasarc.list_catalogs()
    assert cat_list_get.equals(cat_list_stash) is True
    os.remove("astrostash.db")
