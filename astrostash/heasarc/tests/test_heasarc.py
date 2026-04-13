from astrostash.heasarc import Heasarc
from astropy import units as u
from astropy.coordinates import SkyCoord
import os
import pathlib as pl
import shutil
import pytest
import pandas as pd


@pytest.fixture
def copy_dir_setup():
    dbroot = "astrostash/heasarc/tests/data"
    db = f"{dbroot}/processed-conflict.db"
    dbcopy = f"{dbroot}/processed-conflict-copy.db"
    shutil.copy(db, dbcopy)
    heasarc = Heasarc(dbcopy)
    yield heasarc
    os.remove(dbcopy)


@pytest.fixture
def setup():
    heasarc = Heasarc("astrostash/heasarc/tests/data/processed-conflict.db")
    yield heasarc


@pytest.mark.remote
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


@pytest.mark.remote
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


@pytest.mark.remote
def test_query_object(copy_dir_setup):
    heasarc = Heasarc()
    init_query = heasarc.query_object("crab", catalog="nicermastr")
    assert heasarc.ldb._check_table_exists("nicermastr") is True
    alias_query = heasarc.query_object("PSR B0531+21", catalog="nicermastr")
    pd.testing.assert_frame_equal(init_query, alias_query)
    os.remove("astrostash.db")
    heasarc2 = copy_dir_setup
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


@pytest.mark.remote
def test_query_tap(setup):
    setup.query_tap("SELECT * FROM uhuru4", catalog="uhuru4")
    assert setup.ldb._check_table_exists("uhuru4") is True


@pytest.mark.remote
def test_locate_data(setup):
    crabdf = setup.query_object("PSR B0531+21", catalog="nicermastr")
    products = setup.locate_data(crabdf, "nicermastr")
    expected_columns = ['rowid', 'access_url', 'sciserver', 'aws',
                        'content_length', 'error_message', 'local_id',
                        'location']
    assert products.columns.to_list() == expected_columns
    assert len(products["location"].dropna()) == 0


@pytest.mark.remote
def test_download_data(copy_dir_setup):
    heasarc = copy_dir_setup
    crabdf = heasarc.query_object("PSR B0531+21", catalog="nicermastr")
    products = heasarc.locate_data(crabdf, "nicermastr")
    sel = products.loc[products["rowid"] == "43555"]
    heasarc.download_data(sel, "nicermastr", host="heasarc", location=".")
    local_paths = heasarc.ldb.get_local_data_paths_by_catalog("nicermastr")
    expected_dir = str(pl.Path("./1013010107").resolve())
    dummy_frame = pd.DataFrame({
        "id": [1],
        "catalog": ["nicermastr"],
        "rowid": ["43555"],
        "location": [expected_dir]
    })
    pd.testing.assert_frame_equal(local_paths, dummy_frame)
    shutil.rmtree(expected_dir)


@pytest.mark.remote
def test_stash_full_catalog():
    h = Heasarc()
    h.stash_full_catalog("uhuru4", chunk_size=200)
    assert h.ldb._check_table_exists("uhuru4")
    result = h.query_region(catalog="uhuru4", spatial="all-sky", mode="local")
    assert len(result) == 339
    os.remove("astrostash.db")


def test_query_region_local_cone(setup):
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    result = setup.query_region(position=pos, catalog='nicermastr',
                                radius='0.5deg', mode='local')
    assert not result.empty
    assert len(result) == 188
    assert 'ra' in result.columns
    assert 'dec' in result.columns
    assert 'name' in result.columns


def test_query_region_local_cone_quantity(setup):
    from astropy import units as u
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    result = setup.query_region(position=pos, catalog='nicermastr',
                                radius=0.5 * u.deg, mode='local')
    assert not result.empty
    assert len(result) == 188


def test_query_region_local_allsky(setup):
    result = setup.query_region(catalog='uhuru4',
                                spatial='all-sky', mode='local')
    assert len(result) == 339


def test_query_region_local_no_catalog(setup):
    with pytest.raises(ValueError, match="catalog is required"):
        setup.query_region(mode='local', position='10d 20d', radius='1deg')


def test_query_region_local_no_table(setup):
    with pytest.raises(ValueError, match="does not exist"):
        setup.query_region(catalog='nonexistent', mode='local',
                           spatial='all-sky')


def test_query_region_local_missing_position(setup):
    with pytest.raises(ValueError, match="position is required"):
        setup.query_region(catalog='nicermastr', mode='local',
                           radius='1deg')


def test_query_region_local_default_radius(setup):
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    result = setup.query_region(position=pos, catalog='nicermastr', mode='local')
    assert not result.empty


def test_query_region_local_box(setup):
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    result = setup.query_region(position=pos, catalog='nicermastr',
                                spatial='box', width='0.01deg', mode='local')
    assert not result.empty
    # All results should be within the box
    assert all(result['ra'] >= 83.633 - 0.005)
    assert all(result['ra'] <= 83.633 + 0.005)
    assert all(result['dec'] >= 22.015 - 0.005)
    assert all(result['dec'] <= 22.015 + 0.005)


def test_query_region_local_box_missing_width(setup):
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    with pytest.raises(ValueError, match="width is required"):
        setup.query_region(position=pos, catalog='nicermastr',
                           spatial='box', mode='local')


def test_query_region_local_polygon(setup):
    verts = [(83.62, 22.01), (83.65, 22.01), (83.65, 22.03), (83.62, 22.03)]
    result = setup.query_region(catalog='nicermastr',
                                spatial='polygon',
                                polygon=verts, mode='local')
    assert not result.empty
    # All results should be within the polygon bounds
    assert all(result['ra'] >= 83.62)
    assert all(result['ra'] <= 83.65)
    assert all(result['dec'] >= 22.01)
    assert all(result['dec'] <= 22.03)


def test_query_region_local_polygon_missing_polygon(setup):
    with pytest.raises(ValueError, match="polygon is required"):
        setup.query_region(catalog='nicermastr',
                           spatial='polygon', mode='local')


def test_query_region_local_unknown_spatial(setup):
    with pytest.raises(ValueError, match="Unknown spatial mode"):
        setup.query_region(catalog='nicermastr',
                           spatial='invalid', mode='local')


def test_query_region_local_no_ra_dec(setup):
    # heasarc_catalog_list has no ra/dec columns
    with pytest.raises(ValueError, match="has no 'ra' and 'dec' columns"):
        setup.query_region(catalog='heasarc_catalog_list',
                           spatial='all-sky', mode='local')


def test_query_region_invalid_mode(setup):
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    with pytest.raises(ValueError, match="Unknown mode"):
        setup.query_region(position=pos, catalog='nicermastr',
                           radius='0.5deg', mode='bogus')


def test_get_default_radius_from_meta(setup):
    radius = setup._get_default_radius('nicermastr')
    assert radius.unit == u.deg
    expected = 15.0 * u.arcmin.to(u.deg)
    assert radius.value == pytest.approx(expected, rel=1e-6)


def test_query_region_local_override_default_radius(setup):
    pos = SkyCoord(ra=83.633, dec=22.015, unit='deg')
    result_small = setup.query_region(
        position=pos, catalog='nicermastr', radius='0.01deg', mode='local')
    result_default = setup.query_region(
        position=pos, catalog='nicermastr', mode='local')
    assert len(result_small) < len(result_default)
