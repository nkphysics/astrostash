import pathlib as pl
import os
import sqlite3
import pandas as pd
import time
from datetime import datetime
import hashlib
import json


def sha256sum(query_dict: dict) -> str:
    """
    Computes the SHA-256 hash of query parameters.

    Parameters:
    query_dict: dict, parameters for a query

    Returns:
    str: SHA-256 hash of the query
    """
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

    def close(self):
        """
        Close the database connection.
        """
        return self.conn.close()
