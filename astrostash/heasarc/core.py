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
                      refresh_rate=None) -> pd.DataFrame:
        """
        Gets a DataFrame of all available catalogs in the form of
        (name, description)

        Parameters:
        master: bool, Gets only master catalogs if True, default False

        keywords: str or list, keywords used as search terms for catalogs.
                               Words with a str separated by a space
                               are AND'ed, while words in a list are OR'ed
        refresh_rate: int or None, default = None,
                      time in days before the query should be refreshed

        Returns:
        pd.DataFrame, heasarc catalogs and descriptions
        """
        params = locals().copy()
        del params["self"]
        dbquery = """SELECT name, description FROM heasarc_catalog_list
                     WHERE name IN (
                         SELECT rowid FROM response_rowid_pivot rrp
                         INNER JOIN query_response_pivot qrp
                         ON qrp.responseid = rrp.responseid
                         WHERE queryid = :queryid
                     );"""
        return self.ldb.fetch_sync(self.aq.list_catalogs,
                                   "heasarc_catalog_list",
                                   dbquery,
                                   params,
                                   refresh_rate,
                                   idcol="name")

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

    def query_region(self, position=None, catalog=None,
                     radius=None, refresh_rate=None, **kwargs):
        """
        Queries a catalog at the heasarc for records around a specific
        region

        Parameters:
        position: str, `astropy.coordinates` object with coordinate positions
                        Required if spatial is cone or box.
                        Ignored if spatial is polygon or all-sky.

        catalog: str, catalog name as listed at the heasarc

        radius: str or `~astropy.units.Quantity`,
                search radius

        refresh_rate: int or None, default = None,
                      time in days before the query should be refreshed

        **kwargs: additional kwargs to be passed into
                  astroquery.Heasarc.query_region

        Returns:
        pd.DataFrame, table of catalog's records around the specified region
        """
        params = locals().copy()
        del params["self"]
        if self._check_catalog_exists(catalog):
            dbquery = f"""SELECT * FROM {catalog} WHERE obsid IN (
                              SELECT rowid FROM response_rowid_pivot rrp
                              INNER JOIN query_response_pivot qrp
                              ON qrp.responseid = rrp.responseid
                              WHERE qrp.queryid = :queryid);"""
            return self.ldb.fetch_sync(self.aq.query_region,
                                       catalog,
                                       dbquery,
                                       params,
                                       refresh_rate,
                                       **kwargs)
