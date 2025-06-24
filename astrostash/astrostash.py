import pathlib as pl
import os
import sqlite3
import time
from datetime import datetime


class SQLiteDB:
    def __init__(self, db_name=None):
        db_name = self._get_db_file(db_name)
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

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

    def close(self):
        """
        Close the database connection.
        """
        return self.conn.close()
