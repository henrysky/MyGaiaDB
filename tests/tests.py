import stat
import pytest
import pathlib
import mygaiadb
from mygaiadb.query import LocalGaiaSQL
from mygaiadb.data import download, compile
import pandas as pd


@pytest.mark.order(1)
def test_download():
    download.download_gaia_source(test=True)
    download.download_2mass_best_neightbour(test=True)
    download.download_allwise_best_neightbour(test=True)
    download.download_gaia_astrophysical_parameters(test=True)
    download.download_2mass(test=True)
    # download.download_allwise(test=True)


@pytest.mark.order(2)
def test_compile():
    compile.compile_gaia_sql_db(indexing=False)
    compile.compile_tmass_sql_db(indexing=False)
    # compile.compile_allwise_sql_db(indexing=False)
    # check if database exist
    assert mygaiadb.gaia_sql_db_path.exists()
    assert mygaiadb.tmass_sql_db_path.exists()
    # assert mygaiadb.allwise_sql_db_path.exists()
    # assert database > 2GB
    assert mygaiadb.gaia_sql_db_path.stat().st_size > 2e+9


@pytest.mark.order(3)
def test_user_table():
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.upload_user_table(pd.DataFrame({"source_id": [5188146770731873152, 4611686018427432192, 5764607527332179584]}), "user_table_1")
    result = localdb.query("""SELECT * FROM user_table.user_table_1""")
    assert result["source_id"].tolist() == [5188146770731873152, 4611686018427432192, 5764607527332179584]
    localdb.remove_user_table("user_table_1")
    assert localdb.list_user_tables() == {}

@pytest.mark.order(4)
def test_utilities():
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.list_all_tables()
    localdb.get_table_column("gaiadr3.gaia_source")
    localdb.get_table_column("gaiadr3.tmasspscxsc_best_neighbour")
    localdb.get_table_column("tmass.twomass_psc")


@pytest.mark.order(5)
def test_query():
    query = """
    SELECT * 
    FROM gaiadr3.gaia_source as G
    INNER JOIN gaiadr3.astrophysical_parameters as GA on GA.source_id = G.source_id
    INNER JOIN gaiadr3.tmasspscxsc_best_neighbour as T on G.source_id = T.source_id
    INNER JOIN gaiadr3.allwise_best_neighbour as W on W.source_id = T.source_id  
    INNER JOIN tmass.twomass_psc as TM on TM.designation = T.original_ext_source_id
    WHERE (G.has_xp_continuous = 1)  
    LIMIT 10
    """
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.save_csv(query, "output.csv")
    query_df = localdb.query(query)


@pytest.mark.order(6)
def test_cleanup():
    # undo read-only premission
    mygaiadb.gaia_sql_db_path.chmod(stat.S_IWRITE)
    mygaiadb.tmass_sql_db_path.chmod(stat.S_IWRITE)
    # mygaiadb.allwise_sql_db_path.chmod(stat.S_IWRITE)

    # cleanup to prevent caching
    mygaiadb.gaia_sql_db_path.unlink()
    mygaiadb.tmass_sql_db_path.unlink()
    # mygaiadb.allwise_sql_db_path.unlink()
