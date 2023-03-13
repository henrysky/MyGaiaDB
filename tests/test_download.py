import pytest
import mygaiadb
from mygaiadb.query import LocalGaiaSQL
from mygaiadb.data import download, compile

@pytest.mark.order(1)
def test_download():
    download.download_gaia_source(test=True)
    download.download_2mass_best_neightbour(test=True)
    download.download_allwise_best_neightbour(test=True)


@pytest.mark.order(2)
def test_compile():
    compile.compile_gaia_sql_db(do_indexing=False)


@pytest.mark.order(3)
def test_query():
    localdb = LocalGaiaSQL(load_tmass=False, load_allwise=False)
    print(localdb.query("""SELECT * FROM gaiadr3.gaia_source LIMIT 10"""))
