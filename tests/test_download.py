import pytest
import mygaiadb
from mygaiadb.query import LocalGaiaSQL
from mygaiadb.data import download, compile

@pytest.mark.order(1)
def test_download():
    download.download_gaia_source(test=True)
    download.download_2mass_best_neightbour(test=True)
    download.download_allwise_best_neightbour(test=True)
    download.download_gaia_astrophysical_parameters(test=True)


@pytest.mark.order(2)
def test_compile():
    compile.compile_gaia_sql_db(do_indexing=False)


@pytest.mark.order(3)
def test_query():
    query = """
    SELECT * 
    FROM gaiadr3.gaia_source
    INNER JOIN gaiadr3.astrophysical_parameters as GA on GA.source_id = G.source_id
    INNER JOIN gaiadr3.tmasspscxsc_best_neighbour as T on G.source_id = T.source_id
    INNER JOIN gaiadr3.allwise_best_neighbour as W on W.source_id = T.source_id    
    LIMIT 10
    """
    localdb = LocalGaiaSQL(load_tmass=False, load_allwise=False)
    localdb.save_csv(query, "saved")
    query_df = localdb.query(query)
    print(query_df)
