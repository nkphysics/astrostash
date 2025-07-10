import astroquery.heasarc
from astropy.coordinates import SkyCoord
from astrostash import SQLiteDB
import pandas as pd


class Heasarc:
    def __init__(self, db_name=None):
        self.aq = astroquery.heasarc.Heasarc()
        self.ldb = SQLiteDB(db_name=db_name)

    def list_catalogs(self, *,
                      master=False,
                      keywords=None,
                      refresh_rate=30) -> pd.DataFrame:
        """
        Gets a DataFrame of all available catalogs in the form of
        (name, description)

        Parameters:
        master: bool, Gets only master catalogs if True, default False

        keywords: str or list, keywords used as search terms for catalogs.
                               Words with a str separated by a space
                               are AND'ed, while words in a list are OR'ed
        refresh_rate: int, default = 30,
                      time in days before the query should be refreshed

        Returns:
        pd.DataFrame, heasarc catalogs and descriptions
        """
        params = locals().copy()
        del params["self"]
        dbquery = """SELECT name, description
                     FROM heasarc_catalog_list
                     WHERE query_id == :query_id;"""
        return self.ldb.fetch_sync(self.aq.list_catalogs,
                                   "heasarc_catalog_list",
                                   dbquery,
                                   params,
                                   refresh_rate)

    def _check_catalog_exists(self, catalog: str) -> bool:
        """
        Checks whether or not a catalog exists at the heasarc

        Parameters:
        catalog: str, name of catalog

        Returns:
        bool, True if catalog exists at the heasarc otherwise false
        """
        catalogs = self.list_catalogs()["name"].values
        return catalog in catalogs
