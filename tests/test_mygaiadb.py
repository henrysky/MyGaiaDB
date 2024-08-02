import h5py
import pytest
import mygaiadb
from mygaiadb.query import LocalGaiaSQL
from mygaiadb.spec import yield_xp_coeffs
from mygaiadb import gaia_xp_coeff_h5_path
from mygaiadb.utils import radec_to_ecl
from mygaiadb.data import download, compile
import numpy as np
import pandas as pd
import numpy.testing as npt


@pytest.mark.order(0)
def test_utils():
    # ground truth from gaia archive
    ecl_lon, ecl_lat = radec_to_ecl(251.6199206701881, -51.570525258091074)
    assert np.isclose(ecl_lon, 257.0569564736029)
    assert np.isclose(ecl_lat, -28.95388614797272)

    # ground truth from gaia archive
    ecl_lon, ecl_lat = radec_to_ecl(
        [286.5265406513801, 129.79901099303206, 335.83340230395504, 86.52254525701424],
        [
            47.291529577659965,
            62.501265689962935,
            -2.5196758407656876,
            -70.95210921593667,
        ],
    )
    npt.assert_almost_equal(
        ecl_lon,
        [302.3766885818636, 113.54364730018743, 336.6899780497866, 284.39599199643834],
    )
    npt.assert_almost_equal(
        ecl_lat,
        [68.8807587756507, 42.277113700990235, 7.027983296314586, -85.43324423869875],
    )


@pytest.mark.order(1)
def test_download():
    download.download_gaia_source(test=True)
    download.download_2mass_best_neightbour(test=True)
    download.download_allwise_best_neightbour(test=True)
    download.download_gaia_astrophysical_parameters(test=True)
    download.download_2mass(test=True)
    # download.download_allwise(test=True)
    download.download_catwise(test=True)


@pytest.mark.order(2)
def test_compile():
    compile.compile_gaia_sql_db(indexing=False)
    compile.compile_tmass_sql_db(indexing=False)
    # compile.compile_allwise_sql_db(indexing=False)
    compile.compile_catwise_sql_db(indexing=False)
    # check if database exist
    assert mygaiadb.gaia_sql_db_path.exists()
    assert mygaiadb.tmass_sql_db_path.exists()
    # assert mygaiadb.allwise_sql_db_path.exists()
    # assert database > 2GB
    assert mygaiadb.gaia_sql_db_path.stat().st_size > 2e9


@pytest.mark.order(3)
def test_user_table():
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.upload_user_table(
        pd.DataFrame(
            {
                "source_id": [
                    5188146770731873152,
                    4611686018427432192,
                    5764607527332179584,
                ]
            }
        ),
        "user_table_1",
    )
    result = localdb.query("""SELECT * FROM user_table.user_table_1""")
    assert result["source_id"].tolist() == [
        5188146770731873152,
        4611686018427432192,
        5764607527332179584,
    ]
    localdb.remove_user_table("user_table_1")
    assert localdb.list_user_tables() == {}


@pytest.mark.order(4)
def test_query_utilities():
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.list_all_tables()
    localdb.get_table_column("gaiadr3.gaia_source")
    localdb.get_table_column("gaiadr3.tmasspscxsc_best_neighbour")
    localdb.get_table_column("tmass.twomass_psc")
    localdb.get_table_column("catwise.catwise")


@pytest.mark.order(5)
def test_query():
    # just making sure a complex query like this can run without issue
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
    localdb.save_csv(query, "output.csv", comments=False)
    query_df = localdb.query(query)

    assert len(query_df) == 0, "Query should return 0 rows as this test is incomplete"
    assert "source_id" in query_df.keys(), "Query should contain 'source_id'"


@pytest.mark.order(5)
def test_query_saving():
    # ================= query with new line in both start and end =================
    query = """
    SELECT G.ra, G.dec
    FROM gaiadr3.gaia_source as G
    LIMIT 10
    """
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.save_csv(query, "output.csv", overwrite=True, comments=True)
    query_df = localdb.query(query)

    query_df_from_saved = pd.read_csv("output.csv", comment="#")
    # make sure saved csv has the same result of simply query
    assert np.all(query_df.loc[0] == query_df_from_saved.loc[0])

    # ================= query with new line in start =================
    query = """
    SELECT G.ra, G.dec
    FROM gaiadr3.gaia_source as G
    LIMIT 10"""
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.save_csv(query, "output.csv", overwrite=True, comments=True)
    query_df = localdb.query(query)

    query_df_from_saved = pd.read_csv("output.csv", comment="#")
    # make sure saved csv has the same result of simply query
    assert np.all(query_df.loc[0] == query_df_from_saved.loc[0])

    # ================= query with new line in end =================
    query = """SELECT G.ra, G.dec
    FROM gaiadr3.gaia_source as G
    LIMIT 10
    """
    localdb = LocalGaiaSQL(load_allwise=False)
    localdb.save_csv(query, "output.csv", overwrite=True, comments=True)
    query_df = localdb.query(query)

    query_df_from_saved = pd.read_csv("output.csv", comment="#")
    # make sure saved csv has the same result of simply query
    assert np.all(query_df.loc[0] == query_df_from_saved.loc[0])


@pytest.mark.order(6)
def test_xp_query():
    # ================= Generate dataset =================
    f_xp = h5py.File(gaia_xp_coeff_h5_path, "w")
    # list of possible random source_id
    possible_source_ids = np.random.randint(
        4295806720, 6917528997577384320, size=10000000, dtype=np.int64
    )
    reduced_possible_source_ids = possible_source_ids // 8796093022208
    all_source_ids = []

    for i, j in zip([0, 3001, 6001], [3000, 6000, 9000]):
        good_source_ids = (i <= reduced_possible_source_ids) & (
            reduced_possible_source_ids <= j
        )
        _source_ids = possible_source_ids[good_source_ids]
        all_source_ids.append(_source_ids)
        group_name = f"{i}-{j}"
        f_xp.create_group(group_name)
        f_xp[group_name].create_dataset("source_id", data=_source_ids, dtype=np.int64)
        f_xp[group_name].create_dataset(
            "bp_coefficients", data=np.random.random((np.sum(good_source_ids), 55))
        )
        f_xp[group_name].create_dataset(
            "rp_coefficients", data=np.random.random((np.sum(good_source_ids), 55))
        )
        f_xp[group_name].create_dataset(
            "bp_coefficient_errors",
            data=np.random.random((np.sum(good_source_ids), 55)),
        )
        f_xp[group_name].create_dataset(
            "rp_coefficient_errors",
            data=np.random.random((np.sum(good_source_ids), 55)),
        )
    f_xp.close()
    all_source_ids = np.concatenate(all_source_ids)

    # ================= Test query with unique source id =================
    np.random.shuffle(all_source_ids)

    source_ids_result = np.zeros((len(all_source_ids),), dtype=np.int64)

    for i in yield_xp_coeffs(
        all_source_ids,
        return_errors=True,
        assume_unique=True,
        return_additional_columns=["source_id"],
    ):
        coeffs, idx, coeffs_err, ids = i  # unpack
        source_ids_result[idx] = ids

    assert np.all(all_source_ids == source_ids_result)

    # ================= Test query with repeated source id and unknowns =================
    all_source_ids_w_replacement = np.random.choice(
        np.concatenate(
            [
                all_source_ids,
                np.random.randint(
                    4295806720,
                    6917528997577384320,
                    size=len(all_source_ids) // 2,
                    dtype=np.int64,
                ),
            ]
        ),
        size=len(all_source_ids) * 2,
        replace=True,
    )

    source_ids_result = np.zeros((len(all_source_ids_w_replacement),), dtype=np.int64)

    for i in yield_xp_coeffs(
        all_source_ids_w_replacement,
        return_errors=True,
        assume_unique=False,
        return_additional_columns=["source_id"],
    ):
        coeffs, idx, coeffs_err, ids = i  # unpack
        source_ids_result[idx] = ids

    # 0 represents no source_id found for XP coefficients
    assert np.all(
        (all_source_ids_w_replacement == source_ids_result)[source_ids_result != 0]
    )

    # assert error is raised
    with pytest.raises(Exception):
        for i in yield_xp_coeffs(all_source_ids_w_replacement, assume_unique=True):
            coeffs, idx = i  # unpack
