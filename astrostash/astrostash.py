import pathlib as pl
import os
import sqlite3
import pandas as pd
import time
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


class SQLiteDB:
    def __init__(self, db_name=None):
        db_name = self._get_db_file(db_name)
        self.conn = sqlite3.connect(db_name)
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
        with open("schema/base.sql", "r", encoding='utf-8') as schema:
            self.cursor.execute(schema.read())

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
        return type(self.cursor.fetchone()) != None

    def insert_query(self, query_hash: str, refresh_rate: int) -> int:
        """
        Inserts info related to a query into the queries table

        Parameters:
        query: str, sha256 hash of the query parameters

        refresh_rate: int, number of days since last query date to refresh
                           database with fresh data

        Returns:
        int, id for the specific query
        """
        self.cursor.execute("""
            INSERT INTO queries (
                hash,
                last_queried,
                refresh_rate
            )
            VALUES (
                :hash,
                :last_queried,
                :refresh_rate
            );""", {"hash": query_hash,
                    "last_queried": datetime.today().strftime('%Y-%m-%d'),
                    "refresh_rate": refresh_rate}
            )
        self.conn.commit()
        return self.cursor.lastrowid

    def ingest_table(self, table, name, if_exists="replace") -> None:
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

    def fetch_sync(self, query_func, table_name: str, dbquery: str,
                   query_params: dict, refresh_rate: int,
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
        query_hash = sha256sum(query_params)
        qdf = self.get_query(query_hash)
        qid = None
        del query_params["refresh_rate"]
        if qdf.empty is True:
            # If there is no query matching the hash then the query
            # has not been requested before, so we need to insert the query
            # hash to get a query_id, and then stash the query results in a
            # new data table
            qid = self.insert_query(query_hash, refresh_rate)
            df = query_func(*args,**query_params, **kwargs).to_pandas(index=False)
            df["query_id"] = qid
            self.ingest_table(df, table_name)
        else:
            # If a record exists for the query, get the query_id to
            # use to get the stashed reponse from the astrostash database.
            qid = int(qdf["id"].iloc[0])
        return pd.read_sql(dbquery,
                           self.conn,
                           params={"query_id": qid})

    def close(self):
        """
        Close the database connection.
        """
        return self.conn.close()
