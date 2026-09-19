import astroquery.vizier
from astropy.table import Table
from astrostash import SQLiteDB, PostgresDB
import pandas as pd


class Vizier:

    def __init__(self, db_name=None, *,
                 columns=["*"], column_filters={}, catalog=None,
                 keywords=None, ucd="", row_limit=50,
                 pg_host=None, pg_port=5432, pg_dbname=None,
                 pg_user=None, pg_password=None):
        """
        Parameters
        ----------
        db_name : str or None, optional
            Path to the SQLite database file. If None, the default
            astrostash database location is used.
        columns : list of str, optional
            List of column names to include in query results.
            Default is ``["*"]`` (all columns).
        column_filters : dict, optional
            Dictionary of column constraints where keys are column names
            and values are constraint strings. Default is empty dict.
        catalog : str, list of str, or None, optional
            Default catalog to search. If None, searches all catalogs.
        keywords : str, list, or None, optional
            Keywords to filter catalog search in ``find_catalogs()``.
        ucd : str, optional
            Unified Content Description filter. Default is empty string.
        row_limit : int, optional
            Maximum number of rows to return. Set to -1 for unlimited.
            Default is 50.
        pg_host : str, optional
            PostgreSQL host address.
        pg_port : int, optional
            PostgreSQL port number. Default is 5432.
        pg_dbname : str, optional
            PostgreSQL database name.
        pg_user : str, optional
            PostgreSQL username.
        pg_password : str, optional
            PostgreSQL password.
        """
        # Initialize astroquery Vizier with parameters (skip db and pg params)
        self.aq = astroquery.vizier.Vizier(
            columns=columns,
            column_filters=column_filters,
            catalog=catalog,
            keywords=keywords,
            ucd=ucd,
            row_limit=row_limit
        )

        # PostgreSQL connection parameters
        pg_params = {
            "pg_host": pg_host,
            "pg_dbname": pg_dbname,
            "pg_user": pg_user,
            "pg_password": pg_password,
        }
        pg_provided = any(v is not None for v in pg_params.values())
        missing = [k for k, v in pg_params.items() if v is None]

        if db_name is not None and pg_provided:
            raise ValueError(
                "Cannot specify both db_name and PostgreSQL parameters. "
                "Use db_name for SQLite or pg_* params for PostgreSQL."
            )

        if pg_provided:
            if missing:
                raise ValueError(
                    f"PostgreSQL info is missing: {', '.join(missing)}. "
                    "host, database name, user, password are required."
                )
            self.ldb = PostgresDB(
                host=pg_host,
                port=pg_port,
                dbname=pg_dbname,
                user=pg_user,
                password=pg_password
            )
        else:
            self.ldb = SQLiteDB(db_name=db_name)

        # Initialize instance attributes
        self._columns = columns
        self._column_filters = column_filters
        self._catalog = catalog
        self._keywords = None
        self._ucd = ucd
        self.ROW_LIMIT = row_limit

        # Set keywords if provided
        if keywords:
            self.keywords = keywords

    @property
    def columns(self):
        """
        Columns to include in query results.

        The special keyword 'all' will return ALL columns from ALL
        retrieved tables.
        """
        # Columns need to be immutable but still need to be a list
        return list(tuple(self._columns))

    @columns.setter
    def columns(self, values):
        self._columns = values

    @property
    def column_filters(self):
        """
        Filters to run on individual columns.

        See the Vizier website for details on column filter syntax.
        """
        return self._column_filters

    @column_filters.setter
    def column_filters(self, values):
        self._column_filters = values

    @property
    def catalog(self):
        """
        The default catalog to search.

        If left empty, will search all catalogs.
        """
        return self._catalog

    @catalog.setter
    def catalog(self, values):
        self._catalog = values

    @property
    def keywords(self):
        """
        The set of keywords to filter the Vizier search.
        """
        return self._keywords

    @keywords.setter
    def keywords(self, value):
        self._keywords = value

    @keywords.deleter
    def keywords(self):
        self._keywords = None

    @property
    def ucd(self):
        """
        UCD criteria for filtering catalogs.

        See https://vizier.cds.unistra.fr/vizier/vizHelp/1.htx#ucd
        """
        return self._ucd

    @ucd.setter
    def ucd(self, values):
        self._ucd = values

    def find_catalogs(self, keywords, *,
                      refresh_rate=None,
                      refresh=False) -> pd.DataFrame:
        """
        Search VizieR for catalogs matching keywords.

        Parameters
        ----------
        keywords : str or list of str
            Keywords to search for catalogs. Words in a string
            separated by spaces are AND'ed, while words in a list
            are OR'ed.
        refresh_rate : int or None, optional
            Time in days before the query should be refreshed.
            Default is None.
        refresh : bool, optional
            If True, always fetch from remote. Default is False.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns ``catalog_id`` and ``description``.
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
        Wraps astroquery's find_catalogs to return an astropy Table.

        Returns
        -------
        astropy.table.Table
            Table with columns ``catalog_id`` and ``description``.
        """
        result = self.aq.find_catalogs(**kwargs)
        catalog_ids = []
        descriptions = []
        for catalog_id, resource in result.items():
            catalog_ids.append(catalog_id)
            descriptions.append(resource.description)
        return Table([catalog_ids, descriptions],
                     names=["catalog_id", "description"])
