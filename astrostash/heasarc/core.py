import astroquery.heasarc
from astropy.coordinates import SkyCoord
from astrostash import SQLiteDB
from astrostash import sha256sum
import pandas as pd


class Heasarc:
    def __init__(self, db_name=None):
        self.aq = astroquery.heasarc.Heasarc()
        self.ldb = SQLiteDB(db_name=db_name)

    def list_catalogs(self, *,
                      master=False,
                      keywords=None,
                      refresh_rate=30) -> pd.DataFrame:
        """
        Gets a DataFrame of all available catalogs in the form of
        (name, description)

        Parameters:
        master: bool, Gets only master catalogs if True, default False

        keywords: str or list, keywords used as search terms for catalogs.
                               Words with a str separated by a space
                               are AND'ed, while words in a list are OR'ed
        refresh_rate: int, default = 30,
                      time in days before the query should be refreshed

        Returns:
        pd.DataFrame, heasarc catalogs and descriptions
        """
        query_hash = sha256sum({"catalog": "heasarc_catalog_list",
                                "master": master,
                                "keywords": keywords})
        qdf = self.ldb.get_query(query_hash)
        qid = None
        if qdf.empty is True:
            # If there is no query matching the hash then the query
            # has not been requested before, so we need to insert the query
            # hash to get a query_id, and then stash the query results in a
            # new data table
            qid = self.ldb.insert_query(query_hash, refresh_rate)
            list_df = self.aq.list_catalogs(
                master=master,
                keywords=keywords).to_pandas(index=False)
            list_df["query_id"] = qid
            self.ldb.ingest_table(list_df, "heasarc_catalog_list")
        else:
            # If a record exists for the query, get the query_id to
            # use to get the stashed reponse from the astrostash database.
            qid = int(qdf["id"].iloc[0])
        return pd.read_sql("""SELECT name, description
                              FROM heasarc_catalog_list
                              WHERE query_id == :query_id;""",
                           self.ldb.conn,
                           params={"query_id": qid})
