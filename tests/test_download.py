import pytest
import mygaiadb
from mygaiadb.data import download, compile


def test_download():
    download.download_gaia_source(test=True)
    download.download_2mass_best_neightbour(test=True)
    download.download_allwise_best_neightbour(test=True)


@pytest.mark.dependency(depends=["test_download"])
def test_compile():
    compile.compile_gaia_sql_db(do_indexing=False)
