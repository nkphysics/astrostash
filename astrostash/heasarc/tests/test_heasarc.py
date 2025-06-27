from astrostash.heasarc import Heasarc


def test_list_catalogs():
    heasarc = Heasarc()
    assert "nicermastr" in heasarc.list_catalogs()["name"]
