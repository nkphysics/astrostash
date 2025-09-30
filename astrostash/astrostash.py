import pathlib as pl
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import hashlib
import json
import astropy


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


def needs_refresh(last_refreshed: str, refresh_rate: int) -> bool:
    """
    Determins a if a refresh is needed based off of the set refresh rate and
    the last refresh date

    Parameters:
    last_refreshed: str, date of last refresh in format YYYY-MM-DD

    refresh_rate: int, number of days before a refresh in needed

    Returns:
    bool, True if refresh is needed, False if not
    """
    need = False
    today = datetime.today().date()
    last = datetime.strptime(last_refreshed, '%Y-%m-%d').date()
    if (today - last).days >= refresh_rate:
        need = True
    return need


class SQLiteDB:
    def __init__(self, db_name=None):
        self.db_name = self._get_db_file(db_name)
        self.conn = sqlite3.connect(self.db_name)
        self.aconn = create_engine(f"sqlite:///{self.db_name}")
        self.cursor = self.conn.cursor()
        self._create_schema()

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

    def _create_schema(self):
        """
        Creates initial schema for the database
        """
        with open(
            "astrostash/schema/base.sql",
            "r",
            encoding='utf-8'
        ) as schema:
            self.cursor.executescript(schema.read())

    def get_query(self, query_hash: str) -> pd.DataFrame:
        """
        Gets the query id (if it exists) based of the query parameters (hash)

        Parameters:
        query_hash: str, unique sha256 hash of the query

        Returns:
        pd.DataFrame, reference info for the query (if record exists)
                      empty DataFrame if not queryied before
        """
        stashref = pd.read_sql("""SELECT * FROM queries
                                  WHERE hash = :query_hash""",
                               self.conn,
                               params={"query_hash": query_hash})
        return stashref

    def get_refresh_rate(self, qid: int) -> int | None:
        """
        Gets the refresh rate (in days) associated with a query id (if exists)
        If no refresh rate exists returns None

        Parameters:
        qid: int, id associated with a unique query

        Returns:
        int, refresh rate in days or None if no refresh rate exists
        """
        self.cursor.execute("""SELECT refresh_rate FROM queries
                               WHERE id = :qid;""",
                            {"qid": qid})
        refresh_rate = self.cursor.fetchone()
        try:
            return refresh_rate[0]
        except TypeError:
            return refresh_rate

    def _check_table_exists(self, name: str) -> bool:
        """
        Checks to ensure that a user specified table exists in the database

        Parameters:
        name: str, name of table to check if it exists

        Returns:
        bool, True if table exists (should be self explanatory)
        """
        self.cursor.execute("""SELECT 1 FROM sqlite_master
                               WHERE type='table' AND
                               name = :name LIMIT 1;""",
                            {"name": name})
        return self.cursor.fetchone() is not None

    def get_columns(self, tablename: str) -> list:
        """
        Gets all the column names for a specified table

        Parameters:
        tablename: str, name of table to get the columns from

        Returns:
        list, names of all columns from the specified table
        """
        if self._check_table_exists(tablename):
            self.cursor.execute(
                "SELECT name FROM pragma_table_info(:tablename);",
                {"tablename": tablename}
                )
            return [i[0] for i in self.cursor.fetchall()]
        else:
            raise ValueError(f"{tablename} does not exist in {self.db_name}")

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
        self.cursor.execute("""
            INSERT INTO queries (
                hash,
                last_refreshed,
                refresh_rate
            )
            VALUES (
                :hash,
                :last_refreshed,
                :refresh_rate
            );""", {"hash": query_hash,
                    "last_refreshed": datetime.today().strftime('%Y-%m-%d'),
                    "refresh_rate": refresh_rate}
            )
        self.conn.commit()
        return self.cursor.lastrowid

    def _get_response_id(self, rhash: str) -> int | None:
        """
        Checks to see of the response has already been seen previously

        Parameter:
        rhash: str, hash of response

        Returns:
        int or None, id associated with hash that already exists in the
                     database, None if no record of the response hash exists
        """
        self.cursor.execute("""SELECT id FROM responses
                               WHERE hash = :hash;""",
                            {"hash": rhash})
        return self.cursor.fetchone()

    def insert_response(self, response_hash: str) -> int:
        """
        Hashes and then inserts response hash into the responses table

        Parameters:
        response_hash: str, SHA-256 hash of a response data table

        Returns:
        int, id associated with the response after insertion
        """
        self.cursor.execute(
            """INSERT INTO responses (hash) VALUES (:hash);""",
            {"hash": response_hash})
        self.conn.commit()
        return self.cursor.lastrowid

    def insert_query_response_pivot(self, qid: int, rid: int) -> None:
        """
        Inserts a queryid, responseid pair to the respective pivot table

        Parameters:
        qid: int, query id from queries table

        rid: int, response id from the responses table
        """
        self.cursor.execute(
            """ INSERT OR IGNORE INTO query_response_pivot (
                queryid,
                responseid
            )
            VALUES (
                :qid,
                :rid
            );""",
            {"qid": qid, "rid": rid})
        self.conn.commit()

    def _check_query_response_link(self, qid: int, rid: int) -> int:
        """
        Checks the existance of a link between a query and response id

        Parameters:
        qid: int, query id

        rid: int, response id

        Returns:
        int, 1 if exists 0 if it does not exist
        """
        self.cursor.execute(
            """SELECT EXISTS(
                SELECT 1 FROM query_response_pivot
                WHERE queryid = :qid AND responseid = :rid
            );""",
            {"qid": qid, "rid": rid})
        return self.cursor.fetchone()[0]

    def insert_response_rowid_pivot(self,
                                    responseid: int,
                                    rowid: str) -> None:
        """
        Inserts a response id and generic rowid pair

        Parameters:
        responseid: int, response id from responses table

        rowid: str, id associated with a unique row (obsid, name, doi)
                    of an external table (nicermastr, heasarc_catalog_list)
        """
        self.cursor.execute(
            """ INSERT INTO response_rowid_pivot (
                responseid,
                rowid
            )
            VALUES (
                :responseid,
                :rowid
            );""",
            {"responseid": responseid, "rowid": rowid})
        self.conn.commit()

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
                     self.conn,
                     if_exists=if_exists,
                     index=False)
        self.conn.commit()

    def update_last_refreshed(self, qid: int) -> int:
        """
        Updates an existing query's last_refreshed date

        Parameters:
        qid: int, query id

        Returns:
        int, query id which was updated
        """
        self.cursor.execute("""UPDATE queries
                               SET last_refreshed = :last_refreshed
                               WHERE id = :id""",
                            {"last_refreshed": datetime.today()
                                                       .strftime('%Y-%m-%d'),
                             "id": qid})
        self.conn.commit()
        return self.cursor.lastrowid

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
        self.cursor.execute("""UPDATE queries
                               SET refresh_rate = :refresh_rate
                               WHERE id = :id""",
                            {"refresh_rate": refresh_rate,
                             "id": qid})
        self.conn.commit()
        return self.cursor.lastrowid

    def fetch_sync(self, query_func, table_name: str,
                   dbquery: str, query_params: dict,
                   refresh_rate: int | None, idcol: str = "__row",
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
        del query_params["refresh_rate"], query_params["refresh"]
        query_hash = sha256sum(query_params)
        qdf = self.get_query(query_hash)
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
        if qdf.empty is True or refresh is True:
            # If there is no query matching the hash then the query
            # has not been requested before, so we need to insert the query
            # hash to get a queryid, and then stash the query results in a
            # new data table
            if qid is None:
                qid = self.insert_query(query_hash, refresh_rate)
            else:
                self.update_last_refreshed(qid)
            try:
                df = query_func(*args,
                                **query_params,
                                **kwargs).to_pandas(index=False)
            except AttributeError:
                df = query_func(*args,
                                **query_params,
                                **kwargs).to_table().to_pandas(index=False)
            response_hash = make_result_hash(df)
            rid = self._get_response_id(response_hash)
            if rid is None:
                rid = self.insert_response(response_hash)
                self.insert_query_response_pivot(qid, rid)
                for rowid in df[idcol].values:
                    self.insert_response_rowid_pivot(rid, rowid)
            ta_exists = self._check_table_exists(table_name)
            if ta_exists is True:
                dd1 = pd.read_sql_table(table_name, self.aconn)
                dd2 = pd.merge(df, dd1, how="left", indicator=True)
                changes = dd2[
                    dd2["_merge"] == "left_only"
                    ].drop(columns="_merge")
                if len(changes) > 0:
                    diffs = dd1[dd1[idcol].isin(changes[idcol])]
                    if len(diffs) > 0:
                        idxs = diffs.index[0]
                        dd1 = dd1.drop(idxs)
                    updated_table = pd.merge(dd1, df, how="outer")
                    self.ingest_table(updated_table,
                                      table_name,
                                      if_exists="replace")
                else:
                    self.ingest_table(changes, table_name)
            else:
                self.ingest_table(df, table_name)
        return pd.read_sql(dbquery,
                           self.conn,
                           params={"queryid": qid})

    def close(self):
        """
        Close the database connection.
        """
        return self.conn.close()
