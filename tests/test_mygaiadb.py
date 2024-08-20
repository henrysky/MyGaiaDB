import h5py
import pytest
import mygaiadb
from mygaiadb.query import LocalGaiaSQL, DustCallback, ZeroPointCallback, LambdaCallback
from mygaiadb.spec import yield_xp_coeffs
from mygaiadb import gaia_xp_coeff_h5_path
from mygaiadb.utils import radec_to_ecl
from mygaiadb.data import download, compile
import numpy as np
import pandas as pd
import numpy.testing as npt


@pytest.fixture(scope="module")
def localdb():
    return LocalGaiaSQL(load_allwise=False)


@pytest.fixture(scope="module")
def avaliable_source_ids():
    source_ids = []
    with h5py.File(gaia_xp_coeff_h5_path, "r") as f:
        for i in f.keys():
            source_ids.append(f[i]["source_id"][()])
    source_ids = np.concatenate(source_ids)
    return source_ids


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
    download.download_gaia_xp_continuous(test=True)
    download.download_gaia_xp_sampled(test=True)
    download.download_gaia_rvs(test=True)
    download.download_2mass_best_neightbour(test=True)
    download.download_allwise_best_neightbour(test=True)
    download.download_gaia_astrophysical_parameters(test=True)
    download.download_2mass(test=True)
    # download.download_allwise(test=True)
    download.download_catwise(test=True)


@pytest.mark.order(2)
def test_compile():
    compile.compile_gaia_sql_db(indexing=False)
    with pytest.raises(Exception):
        # assert that one need to compile_xp_continuous_h5() first
        compile.compile_xp_continuous_allinone_h5(save_correlation_matrix=False)
    compile.compile_xp_continuous_h5(save_correlation_matrix=True)
    compile.compile_xp_continuous_allinone_h5(save_correlation_matrix=False)
    compile.compile_tmass_sql_db(indexing=False)
    # compile.compile_allwise_sql_db(indexing=False)
    compile.compile_catwise_sql_db(indexing=False)
    # check if database exist
    assert mygaiadb.gaia_sql_db_path.exists()
    assert mygaiadb.gaia_xp_coeff_h5_path.exists()
    assert mygaiadb.tmass_sql_db_path.exists()
    # assert mygaiadb.allwise_sql_db_path.exists()
    # assert database > 2GB
    assert mygaiadb.gaia_sql_db_path.stat().st_size > 2e9


@pytest.mark.order(3)
def test_user_table(localdb):
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
def test_query_utilities(localdb):
    localdb.list_all_tables()
    localdb.get_table_column("gaiadr3.gaia_source")
    localdb.get_table_column("gaiadr3.tmasspscxsc_best_neighbour")
    localdb.get_table_column("tmass.twomass_psc")
    localdb.get_table_column("catwise.catwise")


@pytest.mark.order(5)
def test_query(localdb):
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
    localdb.save_csv(query, "output.csv", comments=False)
    query_df = localdb.query(query)

    assert len(query_df) == 0, "Query should return 0 rows as this test is incomplete"
    assert "source_id" in query_df.keys(), "Query should contain 'source_id'"


@pytest.mark.order(5)
def test_query_saving(localdb):
    # ================= query with new line in both start and end =================
    query = """
    SELECT G.ra, G.dec
    FROM gaiadr3.gaia_source as G
    LIMIT 10
    """
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
    localdb.save_csv(query, "output.csv", overwrite=True, comments=True)
    # make sure overwriting is not allowed
    with pytest.raises(Exception):
        localdb.save_csv(query, "output.csv", overwrite=False, comments=True)
    query_df = localdb.query(query)

    query_df_from_saved = pd.read_csv("output.csv", comment="#")
    # make sure saved csv has the same result of simply query
    assert np.all(query_df.loc[0] == query_df_from_saved.loc[0])


@pytest.mark.order(6)
@pytest.mark.parametrize(
    "return_errors,assume_unique,return_additional_columns,replacement",
    [
        # Test query with unique source id
        (True, True, ["source_id"], False),
        # Test query with repeated source id
        (True, False, ["source_id"], True),
        # Test query with repeated source id but assume unique
        (True, True, ["source_id"], True,),
    ],
)
def test_xp_query(
    avaliable_source_ids,
    return_errors,
    assume_unique,
    return_additional_columns,
    replacement,
):
    all_source_ids = np.random.choice(
        avaliable_source_ids, size=len(avaliable_source_ids), replace=replacement
    )
    source_ids_result = np.zeros((len(all_source_ids),), dtype=np.int64)
    if assume_unique and replacement:  # assert error is raised
        with pytest.raises(Exception):
            for i in yield_xp_coeffs(
                all_source_ids,
                return_errors=return_errors,
                assume_unique=assume_unique,
                return_additional_columns=return_additional_columns,
            ):
                coeffs, idx, coeffs_err, ids = i
    else:
        for i in yield_xp_coeffs(
            all_source_ids,
            return_errors=return_errors,
            assume_unique=assume_unique,
            return_additional_columns=return_additional_columns,
        ):
            coeffs, idx, coeffs_err, ids = i
            source_ids_result[idx] = ids
        # assert source_id
        if not replacement:
            assert np.all(source_ids_result == all_source_ids)
        else:
            # if there are repeated source_id, then only assert those non-zero source_id
            assert np.all(source_ids_result[source_ids_result != 0] == all_source_ids[source_ids_result != 0])


@pytest.mark.order(7)
def test_query_callback(localdb):
    # ================= Test custom Callback =================
    query = """
    SELECT G.source_id, G.ra, G.dec
    FROM gaiadr3.gaia_source as G
    LIMIT 10
    """
    ra_conversion = LambdaCallback(
        new_col_name="ra_rad", func=lambda ra: ra / 180 * np.pi
    )
    localdb.save_csv(
        query, "output.csv", overwrite=True, callbacks=[ra_conversion], comments=True
    )
    # make sure the query result has the same result with numpy angle conversion
    query_df = localdb.query(query, callbacks=[ra_conversion])
    npt.assert_allclose(query_df["ra"] / 180 * np.pi, query_df["ra_rad"])

    # ================= Test custom DustCallback =================
    query = """
    SELECT G.source_id, G.ra, G.dec, G.parallax, G.phot_bp_mean_mag, G.nu_eff_used_in_astrometry, G.pseudocolour, G.astrometric_params_solved
    FROM gaiadr3.gaia_source as G
    LIMIT 10
    """
    # adding zero-point corrected parallax using official Gaia DR3 parallax zero-point python package
    zp_callback = ZeroPointCallback(new_col_name="parallax_w_zp")
    # adding SFD E(B-V) in 2MASS H band filter using mwdust python package
    sfd_dust_callback = DustCallback(
        new_col_name="sfd_ah", filter="2MASS H", dustmap="SFD"
    )
    dust3d_callback = DustCallback(new_col_name="drimmel03_ebv", dustmap="Drimmel03")
    localdb.save_csv(
        query,
        "output.csv",
        overwrite=True,
        callbacks=[zp_callback, sfd_dust_callback, dust3d_callback],
    )
