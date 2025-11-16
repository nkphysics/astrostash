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
                      refresh_rate=None,
                      refresh=False) -> pd.DataFrame:
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

        refresh: bool, default = False
                 Toggles call to the heasarc to refresh the table names
                 response if True

        Returns:
        pd.DataFrame, heasarc catalogs and descriptions
        """
        params = locals().copy()
        del params["self"]
        return self.ldb.fetch_sync(self.aq.list_catalogs,
                                   "heasarc_catalog_list",
                                   params,
                                   refresh_rate,
                                   idcol="name",
                                   refresh=refresh)

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
                     radius=None, refresh_rate=None,
                     refresh=False, **kwargs) -> pd.DataFrame:
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

        refresh: bool, default = False
                 Toggles call to the heasarc to refresh the query response
                 if True

        **kwargs: additional kwargs to be passed into
                  astroquery.Heasarc.query_region

        Returns:
        pd.DataFrame, table of catalog's records around the specified region
        """
        params = locals().copy()
        del params["self"]
        if self._check_catalog_exists(catalog):
            return self.ldb.fetch_sync(self.aq.query_region,
                                       catalog,
                                       params,
                                       refresh_rate,
                                       refresh=refresh,
                                       **kwargs)

    def query_object(self, object_name, catalog=None,
                     radius=None, refresh_rate=None,
                     refresh=False, **kwargs) -> pd.DataFrame:
        """
        Queries a catalog at the heasarc for records around a specific
        object/source

        Parameters:
        object_name: str, object name (e.x. PSR B0531+21)

        catalog: str, optional, catalog name as listed at the heasarc

        radius: str or `~astropy.units.Quantity`, optional
                search radius

        refresh_rate: int or None, default = None, optional,
                      time in days before the query should be refreshed

        refresh: bool, default = False, optional
                 Toggles call to the heasarc to refresh the query response
                 if True

        Returns:
        pd.DataFrame, table of catalog's records for the specified object
        """
        pos = SkyCoord.from_name(object_name)
        return self.query_region(position=pos,
                                 catalog=catalog,
                                 radius=radius,
                                 refresh_rate=refresh_rate,
                                 refresh=refresh,
                                 **kwargs)

    def query_tap(self, query: str, catalog: str, maxrec=None,
                  refresh_rate=None, refresh=False) -> pd.DataFrame:
        """
        Queries the HEASARC's Xamin TAP using ADQL

        Parameters:
        query: str, ADQL query

        catalog: str, catalog table name to stash the data to

        maxrec : int or None (default), optional,
                 maximum number of records to return

        Returns:
        pd.DataFrame, response from HEASARC for the ADQL query
        """
        params = locals().copy()
        del params["self"]
        if self._check_catalog_exists(catalog):
            del params["catalog"]
            return self.ldb.fetch_sync(self.aq.query_tap,
                                       catalog,
                                       params,
                                       refresh_rate,
                                       refresh=refresh)
