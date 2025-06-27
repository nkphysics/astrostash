import astroquery.heasarc
from astropy.coordinates import SkyCoord
from astrostash import SQLiteDB


class Heasarc:
    def __init__(self):
        self.aq = astroquery.heasarc.Heasarc()

    def list_catalogs(self, *, master=False, keywords=None):
        return self.aq.list_catalogs(master=master, keywords=keywords)
