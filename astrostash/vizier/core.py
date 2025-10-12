import astroquery.Vizier
from astrostash import SQLiteDB
import pandas as pd


class Vizier:
    def __init__(self, db_name=None):
        self.aq = astroquery.vizier.Vizier()
        self.ldb = SQLiteDB(db_name=db_name)
