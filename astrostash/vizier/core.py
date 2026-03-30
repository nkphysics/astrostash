import astroquery.vizier as Vizier
from astropy.table import Table
from astrostash import SQLiteDB
import pandas as pd


class Vizier:
    def __init__(self, db_name=None):
        import astroquery.vizier as _vizier
        self.aq = _vizier.Vizier()
        self.ldb = SQLiteDB(db_name=db_name)

    def find_catalogs(self, keywords, *,
                      refresh_rate=None,
                      refresh=False) -> pd.DataFrame:
        """
        Search VizieR for catalogs matching keywords.

        Parameters:
        keywords: str or list, keywords to search for catalogs

        refresh_rate: int or None, default = None,
                      time in days before the query should be refreshed

        refresh: bool, default = False
                 If True, always fetch from remote

        Returns:
        pd.DataFrame, columns: catalog_id, description
        """
        params = locals().copy()
        params.pop("self", None)
        return self.ldb.fetch_sync(
            self._find_catalogs_func,
            "vizier_catalog_list",
            params,
            refresh_rate,
            idcol="catalog_id",
            refresh=refresh
        )

    def _find_catalogs_func(self, **kwargs) -> Table:
        """
        Wraps astroquery's find_catalogs to return an astropy Table
        with columns: catalog_id, description.
        """
        result = self.aq.find_catalogs(**kwargs)
        catalog_ids = []
        descriptions = []
        for catalog_id, resource in result.items():
            catalog_ids.append(catalog_id)
            descriptions.append(resource.description)
        return Table([catalog_ids, descriptions],
                     names=["catalog_id", "description"])
