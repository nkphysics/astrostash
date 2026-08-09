import logging
import astroquery.heasarc
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table
from astrostash import SQLiteDB
import pandas as pd
import pathlib as pl
import time


class Heasarc:
    def __init__(self, db_name=None, *,
                 pg_host=None,
                 pg_port=5432,
                 pg_dbname=None,
                 pg_user=None,
                 pg_password=None):
        """
        Create a Heasarc instance backed by SQLite or PostgreSQL.

        Parameters
        ----------
        db_name : str or None, optional
            Path to the SQLite database file. If None, the default
            astrostash database location is used.
        pg_host : str, optional
            PostgreSQL host address
        pg_port : int, optional
            PostgreSQL port number. Default is 5432
        pg_dbname : str, optional
            PostgreSQL database name
        pg_user : str, optional
            PostgreSQL username
        pg_password : str, optional
            PostgreSQL password
        """
        self.aq = astroquery.heasarc.Heasarc()

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
            from astrostash import PostgresDB
            self.ldb = PostgresDB(
                host=pg_host,
                port=pg_port,
                dbname=pg_dbname,
                user=pg_user,
                password=pg_password
            )
        else:
            self.ldb = SQLiteDB(db_name=db_name)

    def list_catalogs(self, *,
                      master=False,
                      keywords=None,
                      refresh_rate=None,
                      refresh=False) -> pd.DataFrame:
        """
        Get a DataFrame of all available HEASARC catalogs.

        Parameters
        ----------
        master : bool, optional
            If True, return only master catalogs. Default is False.
        keywords : str or list of str, optional
            Search terms for filtering catalogs. Words in a string
            separated by spaces are AND'ed, while words in a list
            are OR'ed.
        refresh_rate : int or None, optional
            Time in days before the query should be refreshed.
            Default is None.
        refresh : bool, optional
            If True, force a remote fetch to refresh the catalog list.
            Default is False.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns ``name`` and ``description``.
        """
        params = locals().copy()
        params.pop("self", None)
        return self.ldb.fetch_sync(self.aq.list_catalogs,
                                   "heasarc_catalog_list",
                                   params,
                                   refresh_rate,
                                   idcol="name",
                                   refresh=refresh)

    def _check_catalog_exists(self, catalog: str) -> bool:
        """
        Check whether a catalog exists at the HEASARC.

        Parameters
        ----------
        catalog : str
            Name of the catalog to check.

        Returns
        -------
        bool
            True if the catalog exists at the HEASARC
        """
        catalogs = self.list_catalogs()["name"].values
        if catalog not in catalogs:
            raise ValueError(f"Catalog: {catalog} does not exist @ HEASARC")
        return True

    def _get_default_radius(self, catalog: str):
        """
        Get the default search radius for a catalog.

        First attempts to retrieve from astroquery's cached metadata, then
        falls back to the class default if unavailable.

        Parameters
        ----------
        catalog : str
            Name of the catalog.

        Returns
        -------
        `~astropy.units.Quantity`
            The default radius in degrees.
        """
        try:
            radius = self.aq.get_default_radius(catalog)
            return radius.to(u.deg)
        except (IndexError, KeyError):
            return (0.1 * u.deg)

    def query_region(self, position=None, catalog=None,
                     radius=None, refresh_rate=None,
                     mode='standard', spatial='cone',
                     width=None, polygon=None,
                     **kwargs) -> pd.DataFrame:
        """
        Query a HEASARC catalog for records around a specific region.

        Parameters
        ----------
        position : str or `~astropy.coordinates.SkyCoord`, optional
            Coordinate position for the query. Required if ``spatial``
            is ``'cone'`` or ``'box'``. Ignored if ``spatial`` is
            ``'polygon'`` or ``'all-sky'``.
        catalog : str
            Catalog name as listed at the HEASARC.
        radius : str or `~astropy.units.Quantity`, optional
            Search radius for cone queries.
        refresh_rate : int or None, optional
            Time in days before the query should be refreshed.
            Default is None.
        mode : str, optional
            Query mode. ``'standard'`` checks local cache first and
            fetches from remote if no cached data exists. ``'refresh'``
            always fetches from remote and updates the cache. ``'local'``
            queries the local database directly for the specified spatial
            region, bypassing remote calls and query history lookup.
            Default is ``'standard'``.
        spatial : str, optional
            Type of spatial query for local mode: ``'cone'``, ``'box'``,
            ``'polygon'``, or ``'all-sky'``. Ignored if ``mode`` is not
            ``'local'``. Default is ``'cone'``.
        width : str or `~astropy.units.Quantity`, optional
            Width of the box search region. Required when ``spatial``
            is ``'box'`` in local mode.
        polygon : list of tuple, optional
            List of ``(ra, dec)`` pairs in decimal degrees outlining the
            polygon to search. Required when ``spatial`` is ``'polygon'``
            in local mode.
        **kwargs
            Additional keyword arguments passed to the underlying
            HEASARC query method.

        Returns
        -------
        pd.DataFrame
            DataFrame of catalog records around the specified region.
        """
        catalog_existance = self._check_catalog_exists(catalog)
        if mode == 'local' and catalog_existance is True:
            if radius is None and spatial == 'cone':
                radius = self._get_default_radius(catalog)
            return self.ldb.query_region(
                table=catalog,
                position=position,
                spatial=spatial,
                radius=radius,
                width=width,
                polygon=polygon
            )
        if mode not in ('standard', 'refresh'):
            raise ValueError(
                f"Unknown mode: '{mode}'. "
                "Expected 'standard', 'refresh', or 'local'")
        refresh = (mode == 'refresh')
        params = locals().copy()
        params.pop("self", None)
        params.pop("mode", None)
        params.pop("spatial", None)
        params.pop("width", None)
        params.pop("polygon", None)
        params.pop("refresh", None)
        if catalog_existance:
            return self.ldb.fetch_sync(self.aq.query_region,
                                       catalog,
                                       params,
                                       refresh_rate,
                                       refresh=refresh,
                                       **kwargs)

    def query_object(self, object_name, catalog=None,
                     radius=None, refresh_rate=None,
                     mode='standard', spatial='cone',
                     width=None, polygon=None,
                     **kwargs) -> pd.DataFrame:
        """
        Query a HEASARC catalog for records around a specific object.

        Resolves the object name to coordinates using
        `~astropy.coordinates.SkyCoord.from_name`, then delegates to
        `query_region`.

        Parameters
        ----------
        object_name : str
            Name of the astronomical object (e.g. ``'PSR B0531+21'``).
        catalog : str, optional
            Catalog name as listed at the HEASARC.
        radius : str or `~astropy.units.Quantity`, optional
            Search radius for the query.
        refresh_rate : int or None, optional
            Time in days before the query should be refreshed.
            Default is None.
        mode : str, optional
            Query mode: ``'standard'``, ``'refresh'``, or ``'local'``.
            See `query_region` for details. Default is ``'standard'``.
        spatial : str, optional
            Spatial query type for local mode: ``'cone'``, ``'box'``,
            ``'polygon'``, or ``'all-sky'``. Default is ``'cone'``.
        width : str or `~astropy.units.Quantity`, optional
            Width of the box search region for local mode.
        polygon : list of tuple, optional
            List of ``(ra, dec)`` pairs in decimal degrees for polygon
            queries in local mode.
        **kwargs
            Additional keyword arguments passed to `query_region`.

        Returns
        -------
        pd.DataFrame
            DataFrame of catalog records for the specified object.
        """
        pos = SkyCoord.from_name(object_name)
        return self.query_region(position=pos,
                                 catalog=catalog,
                                 radius=radius,
                                 refresh_rate=refresh_rate,
                                 mode=mode,
                                 spatial=spatial,
                                 width=width,
                                 polygon=polygon,
                                 **kwargs)

    def query_tap(self, query: str, catalog: str, maxrec=None,
                  refresh_rate=None, refresh=False) -> pd.DataFrame:
        """
        Query the HEASARC Xamin TAP service using ADQL.

        Parameters
        ----------
        query : str
            ADQL query string.
        catalog : str
            Catalog table name to stash the results to.
        maxrec : int or None, optional
            Maximum number of records to return. Default is None
            (server default).
        refresh_rate : int or None, optional
            Time in days before the query should be refreshed.
            Default is None.
        refresh : bool, optional
            If True, force a remote fetch to refresh the results.
            Default is False.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the response from the HEASARC.
        """
        params = locals().copy()
        params.pop("self", None)
        if self._check_catalog_exists(catalog):
            params.pop("catalog", None)
            return self.ldb.fetch_sync(self.aq.query_tap,
                                       catalog,
                                       params,
                                       refresh_rate,
                                       refresh=refresh)

    def locate_data(self,
                    result_table: pd.DataFrame,
                    catalog: str) -> pd.DataFrame:
        """
        Get download links and local paths to HEASARC data products.

        Merges remote data product locations with any previously
        downloaded local paths stored in the database.

        Parameters
        ----------
        result_table : pd.DataFrame
            Results from a previous `query_region`, `query_object`,
            or `query_tap` call.
        catalog : str
            Name of the catalog the results came from.

        Returns
        -------
        pd.DataFrame
            DataFrame containing download links and local paths
            for the data products.
        """
        aq_table = Table.from_pandas(result_table)
        remote_df = self.aq.locate_data(aq_table, catalog).to_pandas()
        remote_df.rename(columns={'ID': 'rowid'}, inplace=True)
        remote_df["rowid"] = remote_df["rowid"].str.extract(r'\?(\d+)',
                                                            expand=False)
        local_df = self.ldb.get_local_data_paths_by_catalog(catalog)
        local_df.drop(columns=["catalog"], inplace=True)
        local_df.rename(columns={'id': 'local_id'}, inplace=True)
        return pd.merge(remote_df, local_df, how="outer")

    def download_data(self, links: pd.DataFrame, catalog: str, *,
                      host="aws", location="."):
        """
        Download data products from a specified host.

        Downloads files using the links from the given host, saves them
        to the specified local directory, and records the paths in the
        local_data_paths table.

        Parameters
        ----------
        links : pd.DataFrame
            DataFrame containing download links (from `locate_data`).
        catalog : str
            Name of the catalog the links come from.
        host : str, optional
            Host to retrieve data products from. Options are
            ``'aws'`` (default, fastest), ``'sciserver'``, or
            ``'heasarc'``.
        location : str, optional
            Local directory path to download data to. Default is
            the current directory (``'.'``).
        """
        links = Table.from_pandas(links)
        linkcol = host
        location = pl.Path(location).resolve()
        if linkcol == 'heasarc':
            linkcol = "access_url"
        for row in links:
            download_name = row[linkcol].split("/")[-2]
            self.aq.download_data(row, host=host, location=location)
            self.ldb.insert_local_data_path(
                catalog,
                row["rowid"],
                f"{location}/{download_name}")

    def stash_full_catalog(self, catalog: str,
                           chunk_size: int = 20000,
                           max_retries: int = 3) -> None:
        """
        Fetch an entire HEASARC catalog and stash it to the local database.

        This method solves the DALOverflowWarning issue that occurs when
        querying large catalogs with ``SELECT * FROM {catalog}`` by fetching
        in chunks. After stashing, use ``query_region(..., mode='local')`` for
        offline queries.

        Parameters
        ----------
        catalog : str
            Catalog table name (e.g., ``'nicermastr'``).
        chunk_size : int, optional
            Number of rows per query batch. Default is 20000.
        """
        if not self._check_catalog_exists(catalog):
            raise ValueError(f"Catalog '{catalog}' does not exist at HEASARC")
        count_result = self.aq.query_tap(
            f"SELECT COUNT(*) FROM {catalog}").to_table()
        total_rows = int(count_result['count'][0])
        for offset in range(0, total_rows, chunk_size):
            query = (f"SELECT TOP {chunk_size} * FROM {catalog} "
                     f"OFFSET {offset}")
            for attempt in range(max_retries):
                try:
                    self.query_tap(query, catalog, maxrec=chunk_size,
                                   refresh=True)
                    break
                except (ConnectionError, TimeoutError, OSError) as e:
                    if attempt == max_retries - 1:
                        raise RuntimeError(
                            f"Failed to fetch chunk at offset {offset} "
                            f"after {max_retries} retries") from e
                    logging.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for "
                        f"chunk at offset {offset}: {e}. "
                        f"Retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)
