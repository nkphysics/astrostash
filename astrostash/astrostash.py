import pathlib as pl
import sqlite3
from abc import ABC, abstractmethod
from sqlalchemy import (
    MetaData, column, create_engine, inspect, or_, select, table, text
)
import pandas as pd
import numpy as np
from datetime import datetime, date
import hashlib
import json
import astropy
from astropy.coordinates import SkyCoord, Angle
from astropy import units as u
from importlib.resources import files


def _adapt_date_iso(val):
    return val.isoformat()


def _adapt_datetime_iso(val):
    return val.isoformat(" ")


sqlite3.register_adapter(date, _adapt_date_iso)
sqlite3.register_adapter(datetime, _adapt_datetime_iso)


def sha256sum(query_dict: dict) -> str:
    """
    Computes the SHA-256 hash of query parameters.

    Parameters:
    query_dict: dict, parameters for a query

    Returns:
    str: SHA-256 hash of the query
    """
    for key, val in query_dict.items():
        if isinstance(val, astropy.coordinates.SkyCoord):
            query_dict = query_dict.copy()
            query_dict[key] = val.to_string()
    json_str = json.dumps(query_dict, sort_keys=True, ensure_ascii=True)
    hash_obj = hashlib.sha256(json_str.encode('utf-8'))
    return hash_obj.hexdigest()


def make_result_hash(df: pd.DataFrame) -> str:
    """
    Computes a SHA-256 hash of a response

    Parameters:
    df: pd.DataFrame, response table from an external query

    Returns:
    str, SHA-256 hash or response dataframe
    """
    pdhash = pd.util.hash_pandas_object(df).to_dict()
    return sha256sum(pdhash)


def needs_refresh(last_refreshed: str | date, refresh_rate: int) -> bool:
    """
    Determins a if a refresh is needed based off of the set refresh rate and
    the last refresh date

    Parameters:
    last_refreshed: str or date
        Date of last refresh. Accepts a string in YYYY-MM-DD format (SQLite)
        or a datetime.date object (PostgreSQL).

    refresh_rate: int, number of days before a refresh in needed

    Returns:
    bool, True if refresh is needed, False if not
    """
    need = False
    today = date.today()
    if isinstance(last_refreshed, date):
        last = last_refreshed
    else:
        last = datetime.strptime(last_refreshed, '%Y-%m-%d').date()
    if (today - last).days >= refresh_rate:
        need = True
    return need


class BaseDB(ABC):

    @property
    @abstractmethod
    def _dialect_insert(self):
        """Return the dialect-specific insert function."""
        pass

    def _create_schema(self, schema_file: str):
        """
        Load and execute the dialect-specific schema file.

        Parameters:
        schema_file: str, name of the .sql file in the schema directory
        """
        schema = files('astrostash.schema').joinpath(schema_file).read_text()
        for statement in schema.split(';'):
            statement = statement.strip()
            if statement:
                self.sconn.execute(text(statement))
        self.sconn.commit()

    @abstractmethod
    def _check_table_exists(self, name: str) -> bool:
        """Check if a table exists in the database."""
        pass

    @abstractmethod
    def get_columns(self, tablename: str) -> list:
        """Get column names for a table."""
        pass

    @abstractmethod
    def close(self):
        """Close the database connection."""
        pass

    def get_query(self, query_hash: str) -> pd.DataFrame:
        """
        Gets the query id (if it exists) based of the query parameters (hash)

        Parameters:
        query_hash: str, unique sha256 hash of the query

        Returns:
        pd.DataFrame, reference info for the query (if record exists)
                      empty DataFrame if not queryied before
        """
        return pd.read_sql(
            text("SELECT * FROM queries WHERE hash = :query_hash"),
            self.sconn,
            params={"query_hash": query_hash}
        )

    def get_refresh_rate(self, qid: int) -> int | None:
        """
        Gets the refresh rate (in days) associated with a query id (if exists)
        If no refresh rate exists returns None

        Parameters:
        qid: int, id associated with a unique query

        Returns:
        int, refresh rate in days or None if no refresh rate exists
        """
        result = self.sconn.execute(
            text("SELECT refresh_rate FROM queries WHERE id = :qid"),
            {"qid": qid}
        )
        row = result.fetchone()
        try:
            return row[0]
        except TypeError:
            return row

    def _validate_spatial_table(self, table: str) -> None:
        """
        Validates that a table exists and has ra/dec columns

        Parameters:
        table: str, name of table to validate

        Raises:
        ValueError, if table doesn't exist or lacks ra/dec columns
        """
        if not self._check_table_exists(table):
            raise ValueError(
                f"Table '{table}' does not exist in {self.db_name}")
        cols = [c.lower() for c in self.get_columns(table)]
        if "ra" not in cols or "dec" not in cols:
            raise ValueError(
                f"Table '{table}' has no 'ra' and 'dec' columns")

    def _normalize_position(self, position) -> SkyCoord:
        """
        Normalizes a position to a SkyCoord object

        Parameters:
        position: SkyCoord or str, coordinate position

        Returns:
        SkyCoord, normalized coordinate object
        """
        if isinstance(position, SkyCoord):
            return position
        return SkyCoord(position)

    def _parse_angle(self, value) -> float:
        """
        Converts a string or Quantity angle to degrees

        Parameters:
        value: str or astropy.units.Quantity, angle value

        Returns:
        float, angle in degrees
        """
        if isinstance(value, str):
            return Angle(value).deg
        return value.to(u.deg).value

    def _point_in_polygon(self, ra: np.ndarray,
                          dec: np.ndarray,
                          vertices: list) -> np.ndarray:
        """
        Vectorized ray-casting point-in-polygon test

        Parameters:
        ra: np.ndarray, right ascension values in degrees
        dec: np.ndarray, declination values in degrees
        vertices: list of (ra, dec) tuples in degrees outlining the polygon

        Returns:
        np.ndarray, boolean mask where True indicates point is inside polygon
        """
        n = len(vertices)
        inside = np.zeros(len(ra), dtype=bool)
        for i in range(n):
            yi, xi = vertices[i][1], vertices[i][0]
            yj, xj = vertices[(i - 1) % n][1], vertices[(i - 1) % n][0]
            if yi == yj:
                continue
            cond = ((yi > dec) != (yj > dec)) & \
                   (ra < (xj - xi) * (dec - yi) / (yj - yi) + xi)
            inside ^= cond
        return inside

    def insert_query(self, query_hash: str, refresh_rate: int | None) -> int:
        """
        Inserts info related to a query into the queries table

        Parameters:
        query: str, sha256 hash of the query parameters

        refresh_rate: int or None, number of days since last query date to
                                   refresh database with fresh data

        Returns:
        int, id for the specific query
        """
        stmt = (
            self._dialect_insert(self.metadata.tables['queries'])
            .values(
                hash=query_hash,
                last_refreshed=date.today(),
                refresh_rate=refresh_rate
            )
        )
        result = self.sconn.execute(stmt)
        self.sconn.commit()
        return result.inserted_primary_key[0]

    def _get_response_id(self, rhash: str) -> int | None:
        """
        Checks to see of the response has already been seen previously

        Parameter:
        rhash: str, hash of response

        Returns:
        int or None, id associated with hash that already exists in the
                     database, None if no record of the response hash exists
        """
        result = self.sconn.execute(
            text("SELECT id FROM responses WHERE hash = :hash"),
            {"hash": rhash}
        )
        return result.fetchone()

    def insert_response(self, response_hash: str) -> int:
        """
        Hashes and then inserts response hash into the responses table

        Parameters:
        response_hash: str, SHA-256 hash of a response data table

        Returns:
        int, id associated with the response after insertion
        """
        stmt = (
            self._dialect_insert(self.metadata.tables['responses'])
            .values(hash=response_hash)
        )
        result = self.sconn.execute(stmt)
        self.sconn.commit()
        return result.inserted_primary_key[0]

    def insert_query_response_pivot(self, qid: int, rid: int) -> None:
        """
        Inserts a queryid, responseid pair to the respective pivot table

        Parameters:
        qid: int, query id from queries table

        rid: int, response id from the responses table
        """
        stmt = (
            self._dialect_insert(self.metadata.tables['query_response_pivot'])
            .values(queryid=qid, responseid=rid)
            .on_conflict_do_nothing()
        )
        self.sconn.execute(stmt)
        self.sconn.commit()

    def _check_query_response_link(self, qid: int, rid: int) -> int:
        """
        Checks the existance of a link between a query and response id

        Parameters:
        qid: int, query id

        rid: int, response id

        Returns:
        int, 1 if exists 0 if it does not exist
        """
        result = self.sconn.execute(
            text("""SELECT EXISTS(
                SELECT 1 FROM query_response_pivot
                WHERE queryid = :qid AND responseid = :rid
            )"""),
            {"qid": qid, "rid": rid}
        )
        return result.fetchone()[0]

    def insert_response_rowid_pivot(self,
                                    responseid: int,
                                    rowid: list[str]) -> None:
        """
        Inserts response id and rowid pair(s)

        Parameters:
        responseid: int, response id from responses table

        rowid: list of str, id(s) associated with unique row(s)
                    (obsid, name, doi) of an external table
                    (nicermastr, heasarc_catalog_list)
        """
        stmt = (
            self._dialect_insert(self.metadata.tables['response_rowid_pivot'])
            .values(responseid=responseid, rowid=None)
            .on_conflict_do_nothing()
        )
        values = [{"responseid": responseid, "rowid": r} for r in rowid]
        self.sconn.execute(stmt, values)
        self.sconn.commit()

    def _ingest_response_and_links(self, df: pd.DataFrame, qid: int,
                                   idcol: str) -> None:
        """
        Ingests response info and links between response and rowid's in other
        tables in the database

        Parameters
        ----------
        df: pd.DataFrame, response table

        qid: int, query id

        idcol: str, name of id column from response table
        """
        response_hash = make_result_hash(df)
        rid = self._get_response_id(response_hash)
        if rid is None:
            rid = self.insert_response(response_hash)
            self.insert_query_response_pivot(qid, rid)
            self.insert_response_rowid_pivot(rid, df[idcol].tolist())
        else:
            self.insert_query_response_pivot(qid, rid[0])
            existing_rows = pd.read_sql(
                text("""SELECT rowid FROM response_rowid_pivot
                   WHERE responseid = :rid;"""),
                self.sconn,
                params={"rid": rid[0]})
            if not existing_rows.empty:
                new_rowids = set(df[idcol].astype(str).tolist())
                stored_rowids = set(existing_rows["rowid"].tolist())
                missing_rowids = new_rowids - stored_rowids
                if missing_rowids:
                    self.insert_response_rowid_pivot(
                        rid[0],
                        list(missing_rowids)
                    )

    def ingest_table(self, table, name, if_exists="append") -> None:
        """
        Ingests the queried response table into the database with the option
        to either update, append, or fail if it already exists

        Parameters:
        table: pd.DataFrame, table data to be ingested into the database

        name: str, name of the data table

        if_exists: str, optional, how to behave if the table already exists.
                                  (fail, replace, or append)
        """
        table.to_sql(name,
                     self.aconn,
                     if_exists=if_exists,
                     index=False)

    def update_last_refreshed(self, qid: int) -> int:
        """
        Updates an existing query's last_refreshed date

        Parameters:
        qid: int, query id

        Returns:
        int, query id which was updated
        """
        self.sconn.execute(
            text("""UPDATE queries
                    SET last_refreshed = :last_refreshed
                    WHERE id = :id"""),
            {"last_refreshed": date.today(),
             "id": qid}
        )
        self.sconn.commit()
        return qid

    def update_refresh_rate(self, qid: int, refresh_rate: int | None) -> int:
        """
        Updates an existing query record's refresh rate (days)

        Parameters:
        qid: int, query id

        refresh_rate: int or None, new refresh rate in days to be associated
                                   with a query

        Returns:
        int, last accessed queryid that was updated
        """
        self.sconn.execute(
            text("""UPDATE queries
                    SET refresh_rate = :refresh_rate
                    WHERE id = :id"""),
            {"refresh_rate": refresh_rate,
             "id": qid}
        )
        self.sconn.commit()
        return qid

    def _get_queryid(self, qdf: pd.DataFrame, refresh: bool,
                     refresh_rate: int | None) -> tuple:
        """
        Gets query id from given query information and determines if refresh
        (t/f) is warrented

        Parameters
        ----------
        qdf: pd.DataFrame, info for the query (if record exists)
                           empty DataFrame if not queryied before

        refresh: bool, True if refresh toggled on

        refresh_rate: int or None, number of days before refresh is needed

        Returns:
        int, (query id, refresh state)
        """
        try:
            qid = int(qdf["id"].iloc[0])
            q_refresh_rate = self.get_refresh_rate(qid)
            if refresh_rate is not None and refresh_rate != q_refresh_rate:
                q_refresh_rate = refresh_rate
                self.update_refresh_rate(qid, refresh_rate)
            last_refresh_date = qdf["last_refreshed"].iloc[0]
            if q_refresh_rate is not None and refresh is not True:
                refresh = needs_refresh(last_refresh_date, q_refresh_rate)
        except IndexError:
            qid = None
        return qid, refresh

    def _stash_table(self, df: pd.DataFrame,
                     table_name: str, idcol: str) -> None:
        """
        Merges the results of a query into the designated table in the
        database (if exists), or creates a new table and ingests the new data.

        Parameters
        ----------
        df: pd.DataFrame, frame with response data from a query

        table_name: str, name of the table/catalog in the database

        idcol: str, column name of the column to be used for id info
        """
        if idcol not in df.columns:
            raise ValueError(
                f"idcol '{idcol}' not found in response DataFrame. "
                f"Available columns: {list(df.columns)}")

        ta_exists = self._check_table_exists(table_name)
        if not ta_exists:
            self.ingest_table(df, table_name)
            return

        dd1 = pd.read_sql_table(table_name, self.aconn)
        old_ids = set(dd1[idcol].astype(str))
        new_ids = set(df[idcol].astype(str))
        overlapping_ids = old_ids & new_ids
        if overlapping_ids:
            dd1 = dd1[~dd1[idcol].astype(str).isin(overlapping_ids)]
        updated_table = pd.concat([dd1, df], ignore_index=True)
        self.ingest_table(updated_table, table_name, if_exists="replace")

    def _get_stashed_rows(self, catalog: str,
                          qid: int, idcol: str) -> pd.DataFrame:
        """
        Gets the stashed rows associated with a query and response

        Parameters
        ----------
        catalog: str, name of catalog/table

        qid: int, query id

        idcol: str, name of column in catalog/table used for id

        Returns:
        pd.DataFrame, rows of a catalog associated with a query
        """
        if not self._check_table_exists(catalog):
            raise ValueError(f"Catalog: {catalog} does not exist in the db")
        valid_cols = self.get_columns(catalog)
        if idcol not in valid_cols:
            raise ValueError(f"Column {idcol} does not exist in {catalog}")

        cat = table(catalog, *[column(c) for c in valid_cols])
        rrp = table('response_rowid_pivot',
                     column('rowid'), column('responseid'))
        qrp = table('query_response_pivot',
                     column('queryid'), column('responseid'))

        subq = (select(qrp.c.responseid)
                .where(qrp.c.queryid == qid)
                .order_by(qrp.c.responseid.desc())
                .limit(1)
                .scalar_subquery())

        stmt = (select(cat)
                .join(rrp, cat.c[idcol] == rrp.c.rowid)
                .where(rrp.c.responseid == subq))

        return pd.read_sql(stmt, self.sconn)

    def get_local_data_paths_by_catalog(self, catalog: str) -> pd.DataFrame:
        """
        Gets rows of local_data_paths for a specific catalog

        Parameters
        ----------
        catalog: str, name of catalog to filter local_data_paths by

        Returns
        -------
        pd.DataFrame, rows of local_data_paths for a the specified catalog
        """
        query = "SELECT * FROM local_data_paths WHERE catalog = :catalog"
        df = pd.read_sql(text(query), self.sconn, params={"catalog": catalog})
        return df

    def insert_local_data_path(self, catalog: str,
                               rowid: int | str, location: str) -> int:
        """
        Inserts a new record into the local_data_paths table.
        If a record with the same (catalog, rowid, location) already exists,
        it is ignored.

        Parameters
        ----------
        catalog: str, catalog the data product is associated with

        rowid: str, id from the catalog table

        location: str, local path to data product

        Returns
        -------
        int, id for the record of the data path location
        """
        stmt = (
            self._dialect_insert(self.metadata.tables['local_data_paths'])
            .values(catalog=catalog, rowid=rowid, location=location)
            .on_conflict_do_nothing()
        )
        result = self.sconn.execute(stmt)
        self.sconn.commit()

        if result.inserted_primary_key[0] is not None:
            return result.inserted_primary_key[0]

        existing = self.get_local_data_paths_by_catalog(catalog)
        return int(existing[existing['rowid'] == str(rowid)]['id'].iloc[0])

    def fetch_sync(self, query_func, table_name: str,
                   query_params: dict,
                   refresh_rate: int | None,
                   idcol: str = "__row",
                   refresh: bool = False,
                   *args, **kwargs) -> pd.DataFrame:
        """
        Fetches existing data from the user's database if it exists from a
        previous query. Otherwise adds the query reference to the db, executes
        the query function with the passed in function args + kwargs, and
        stashes the results in the db in the table name specified.

        Parameters:
        query_func: function, function to call to execute astroquery function
                              if stashed results do not exist

        table_name: str, table name from user's db

        db_query: str, SQL query to get data from local db table

        *args: args to be passed into query_func (if executed)

        **kwargs: kwargs to be passed into the query_func (if executed)

        Returns:
        pd.DataFrame, table with the results of the query
        """
        query_params.pop("refresh_rate", None)
        query_params.pop("refresh", None)
        query_hash = sha256sum(query_params)
        qdf = self.get_query(query_hash)
        qid, refresh = self._get_queryid(qdf, refresh, refresh_rate)
        if qdf.empty is True or refresh is True:
            # If there is no query matching the hash then the query
            # has not been requested before, so we need to insert the query
            # hash to get a queryid, and then stash the query results in a
            # new data table
            if qid is None:
                qid = self.insert_query(query_hash, refresh_rate)
            else:
                self.update_last_refreshed(qid)
            if refresh and hasattr(query_func, '__self__'):
                query_func.__self__.clear_cache()
            try:
                df = query_func(*args,
                                **query_params,
                                **kwargs).to_pandas(index=False)
            except AttributeError:
                df = query_func(*args,
                                **query_params,
                                **kwargs).to_table().to_pandas(index=False)
            self._ingest_response_and_links(df, qid, idcol)
            # Stash the the external response in the database
            self._stash_table(df, table_name, idcol)
        return self._get_stashed_rows(table_name, qid, idcol)

    def _query_region_box(self, catalog: str, position, width) -> pd.DataFrame:
        coord = self._normalize_position(position)
        cra = float(coord.ra.deg)
        cdec = float(coord.dec.deg)
        half = float(self._parse_angle(width)) / 2.0

        ra_min = cra - half
        ra_max = cra + half
        dec_min = cdec - half
        dec_max = cdec + half

        cols = self.get_columns(catalog)
        cat = table(catalog, *[column(c) for c in cols])

        if ra_min < 0:
            # box crosses 0: match high-RA end OR low-RA end
            stmt = (select(cat)
                    .where(or_(cat.c.ra >= 360 + ra_min, cat.c.ra <= ra_max))
                    .where(cat.c.dec >= dec_min)
                    .where(cat.c.dec <= dec_max))
        elif ra_max > 360:
            # box crosses 360: match high-RA end OR low-RA end
            stmt = (select(cat)
                    .where(or_(cat.c.ra >= ra_min, cat.c.ra <= ra_max - 360))
                    .where(cat.c.dec >= dec_min)
                    .where(cat.c.dec <= dec_max))
        else:
            stmt = (select(cat)
                    .where(cat.c.ra >= ra_min)
                    .where(cat.c.ra <= ra_max)
                    .where(cat.c.dec >= dec_min)
                    .where(cat.c.dec <= dec_max))

        return pd.read_sql(stmt, self.sconn)

    def query_region(self, table: str,
                     position=None,
                     spatial: str = 'cone',
                     radius=None,
                     width=None,
                     polygon: list = None,
                     idcol: str = None) -> pd.DataFrame:
        """
        Queries a local database table for rows within a specified
        astronomical region

        Parameters:
        table : str
            The catalog table to query. The table must have ``ra`` and
            ``dec`` columns.
        position : str or `astropy.coordinates.SkyCoord`
            Gives the position of the center of the cone or box if
            performing a cone or box search. Required if spatial is
            ``'cone'`` or ``'box'``. Ignored if spatial is
            ``'polygon'`` or ``'all-sky'``.
        spatial : str
            Type of spatial query: ``'cone'``, ``'box'``, ``'polygon'``,
            and ``'all-sky'``. Defaults to ``'cone'``.
        radius : str or `~astropy.units.Quantity`,
            The string must be parsable by `~astropy.coordinates.Angle`.
            The appropriate `~astropy.units.Quantity` object from
            `astropy.units` may also be used.
        width : str or `~astropy.units.Quantity`, [Required for
            spatial == ``'box'``]
            The string must be parsable by `~astropy.coordinates.Angle`.
            The appropriate `~astropy.units.Quantity` object from
            `astropy.units` may also be used.
        polygon : list, [Required for spatial is ``'polygon'``]
            A list of ``(ra, dec)`` pairs (as tuples), in decimal degrees,
            outlining the polygon to search in.

        Returns:
        pd.DataFrame, rows from the table that fall within the specified
                      spatial region
        """
        self._validate_spatial_table(table)

        if spatial == 'box':
            if position is None:
                raise ValueError("position is required for box queries")
            if width is None:
                raise ValueError("width is required for box queries")
            return self._query_region_box(table, position, width)

        df = pd.read_sql_table(table, self.aconn)
        if df.empty:
            return df
        ra = df['ra'].to_numpy()
        dec = df['dec'].to_numpy()

        result = df
        if spatial == 'all-sky':
            pass
        elif spatial == 'cone':
            if position is None:
                raise ValueError("position is required for cone queries")
            coord = self._normalize_position(position)
            cra, cdec = coord.ra.deg, coord.dec.deg
            if radius is None:
                raise ValueError("radius is required for local cone queries")
            r_deg = self._parse_angle(radius)
            ra_rad = np.radians(ra)
            dec_rad = np.radians(dec)
            cra_rad = np.radians(cra)
            cdec_rad = np.radians(cdec)
            dlat = dec_rad - cdec_rad
            dlon = ra_rad - cra_rad
            a = np.sin(dlat / 2) ** 2 + \
                np.cos(cdec_rad) * np.cos(dec_rad) * \
                np.sin(dlon / 2) ** 2
            dist_deg = np.degrees(2 * np.arcsin(np.sqrt(a)))
            mask = dist_deg <= r_deg
            result = df[mask].reset_index(drop=True)
        elif spatial == 'polygon':
            if polygon is None:
                raise ValueError("polygon is required for polygon queries")
            inside = self._point_in_polygon(ra, dec, polygon)
            result = df[inside].reset_index(drop=True)
        else:
            raise ValueError(f"Unknown spatial mode: '{spatial}'")

        return result


class SQLiteDB(BaseDB):
    @property
    def _dialect_insert(self):
        from sqlalchemy.dialects.sqlite import insert
        return insert

    def __init__(self, db_name=None):
        self.db_name = self._get_db_file(db_name)
        self.aconn = create_engine(f"sqlite:///{self.db_name}")
        self.sconn = self.aconn.connect()
        self._create_schema('base.sql')
        self.metadata = MetaData()
        self.metadata.reflect(self.aconn)

    def _get_db_file(self, dbpath=None) -> pl.Path:
        """
        Gets or makes a path object for a sqlite database

        Parameters:
        dbpath: optional, None or str, input path to database
        """
        if dbpath is None:
            return pl.Path("astrostash.db").resolve()
        else:
            return pl.Path(dbpath).resolve()

    def _check_table_exists(self, name: str) -> bool:
        """
        Checks to ensure that a user specified table exists in the database

        Parameters:
        name: str, name of table to check if it exists

        Returns:
        bool, True if table exists (should be self explanatory)
        """
        return inspect(self.aconn).has_table(name)

    def get_columns(self, tablename: str) -> list:
        """
        Gets all the column names for a specified table

        Parameters:
        tablename: str, name of table to get the columns from

        Returns:
        list, names of all columns from the specified table
        """
        if not self._check_table_exists(tablename):
            raise ValueError(f"{tablename} does not exist in {self.db_name}")
        return [col['name'] for col in inspect(self.aconn).get_columns(tablename)]

    def close(self):
        """
        Close the database connection.
        """
        self.sconn.close()
        self.aconn.dispose()


class PostgresDB(BaseDB):
    @property
    def _dialect_insert(self):
        from sqlalchemy.dialects.postgresql import insert
        return insert

    def __init__(self, host, port, dbname, user, password):
        self.db_name = dbname
        url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self.aconn = create_engine(url)
        self.sconn = self.aconn.connect()
        self._create_schema('base-postgres.sql')
        self.metadata = MetaData()
        self.metadata.reflect(self.aconn)

    def _check_table_exists(self, name: str) -> bool:
        return inspect(self.aconn).has_table(name)

    def get_columns(self, tablename: str) -> list:
        if not self._check_table_exists(tablename):
            raise ValueError(f"{tablename} does not exist in {self.db_name}")
        cols = inspect(self.aconn).get_columns(tablename)
        return [c['name'] for c in cols]

    def close(self):
        self.sconn.close()
        self.aconn.dispose()
