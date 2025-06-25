import pathlib as pl
import os
import sqlite3
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

    def close(self):
        """
        Close the database connection.
        """
        return self.conn.close()
