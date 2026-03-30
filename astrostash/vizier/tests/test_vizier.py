from astrostash.vizier import Vizier
import os
import pathlib as pl
import pytest
import pandas as pd


TESTDB = "astrostash/vizier/tests/data/test-vizier.db"


@pytest.fixture
def setup():
    dbpath = pl.Path(TESTDB)
    dbpath.parent.mkdir(parents=True, exist_ok=True)
    vizier = Vizier(TESTDB)
    yield vizier


@pytest.fixture
def fresh_setup():
    dbpath = pl.Path(TESTDB)
    dbpath.parent.mkdir(parents=True, exist_ok=True)
    if dbpath.exists():
        os.remove(dbpath)
    vizier = Vizier(TESTDB)
    yield vizier
    if dbpath.exists():
        os.remove(dbpath)


@pytest.mark.remote
def test_find_catalogs(fresh_setup):
    vizier = fresh_setup
    result = vizier.find_catalogs("hot jupiter exoplanet transit")
    assert isinstance(result, pd.DataFrame)
    assert "catalog_id" in result.columns
    assert "description" in result.columns
    assert len(result) > 0
    assert vizier.ldb._check_table_exists("vizier_catalog_list") is True


@pytest.mark.remote
def test_find_catalogs_caching(fresh_setup):
    vizier = fresh_setup
    first = vizier.find_catalogs("hot jupiter exoplanet transit")
    second = vizier.find_catalogs("hot jupiter exoplanet transit")
    pd.testing.assert_frame_equal(first, second)


@pytest.mark.remote
def test_find_catalogs_refresh(fresh_setup):
    vizier = fresh_setup
    first = vizier.find_catalogs("hot jupiter exoplanet transit")
    refreshed = vizier.find_catalogs(
        "hot jupiter exoplanet transit", refresh=True)
    pd.testing.assert_frame_equal(first, refreshed)


@pytest.mark.remote
def test_find_catalogs_keywords_list(fresh_setup):
    vizier = fresh_setup
    result = vizier.find_catalogs(["Mars", "Phobos"])
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_find_catalogs_local(setup):
    vizier = setup
    result = vizier.find_catalogs("hot jupiter exoplanet transit")
    assert isinstance(result, pd.DataFrame)
    assert "catalog_id" in result.columns
    assert "description" in result.columns
    assert len(result) > 0
