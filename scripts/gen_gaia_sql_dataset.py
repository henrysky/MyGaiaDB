import sqlite3
from pathlib import Path
import os
import glob
import tqdm
import numpy as np
import pandas as pd
from datetime import datetime
from mygaiadb import astro_data_path, gaia_sql_db_path

# =================== settings ===================
# The whole script takes about ~24 hours to complete
do_gaia_source_table = True
do_indexing = True
# =================== settings ===================

db_filename = gaia_sql_db_path
Path(db_filename).touch()
conn = sqlite3.connect(db_filename)
c = conn.cursor()

# we have added "grvs_mag" to the table on top of gaia_source_lite on Gaia Archive
# will take ~11 hours to run
if do_gaia_source_table:
    # =================== setup Gaia schema and first two tables ===================
    # this section will take ~30 minutes to run
    for schema in [
        "gaia_source_lite_schema",
        "allwise_best_neighbour_schema",
        "tmasspscxsc_best_neighbour_schema",
    ]:
        schema_filename = os.path.join("scripts", "sql_schema", f"{schema}")

        with open(schema_filename) as f:
            lines = f.read().replace("\n", "")
        c.execute(lines)

    for name in [
        "cross_match/allwise_best_neighbour",
        "cross_match/tmasspscxsc_best_neighbour",
    ]:
        path = os.path.join(astro_data_path, f"gaia_mirror/Gaia/gdr3/{name}/*.csv.gz")

        glob_paths = glob.glob(path)
        for p in tqdm.tqdm(glob_paths):
            # load the data into a Pandas DataFrame
            data = pd.read_csv(p, header=0, sep=",")
            # write the data to a sqlite table
            table_name = name.split("/")[-1]
            data.to_sql(f"{table_name}", conn, if_exists="append", index=False)

    # =================== populate gaia_source lite table ===================

    path = os.path.join(astro_data_path, f"gaia_mirror/Gaia/gdr3/gaia_source/*.csv.gz")
    glob_paths = glob.glob(path)

    # use "Int32" type for int columns with NaN
    for p in tqdm.tqdm(glob_paths):
        dtypes = {
            "source_id": np.int64,
            "random_index": np.int64,
            "ra": np.float64,
            "ra_error": np.float64,
            "dec": np.float64,
            "dec_error": np.float64,
            "parallax": np.float64,
            "parallax_error": np.float64,
            "parallax_over_error": np.float32,
            "pmra": np.float64,
            "pmra_error": np.float64,
            "pmdec": np.float64,
            "pmdec_error": np.float64,
            "ra_dec_corr": np.float32,
            "ra_parallax_corr": np.float32,
            "ra_pmra_corr": np.float32,
            "ra_pmdec_corr": np.float32,
            "dec_parallax_corr": np.float32,
            "dec_pmra_corr": np.float32,
            "dec_pmdec_corr": np.float32,
            "parallax_pmra_corr": np.float32,
            "parallax_pmdec_corr": np.float32,
            "pmra_pmdec_corr": np.float32,
            "astrometric_params_solved": "Int32",
            "nu_eff_used_in_astrometry": np.float32,
            "pseudocolour": np.float64,
            "pseudocolour_error": np.float64,
            "astrometric_matched_transits": "Int32",
            "ipd_gof_harmonic_amplitude": np.float32,
            "ipd_frac_multi_peak": "Int32",
            "ipd_frac_odd_win": "Int32",
            "ruwe": np.float32,
            "phot_g_mean_flux": np.float64,
            "phot_g_mean_flux_over_error": np.float32,
            "phot_g_mean_mag": np.float32,
            "phot_bp_mean_flux": np.float64,
            "phot_bp_mean_flux_over_error": np.float32,
            "phot_bp_mean_mag": np.float32,
            "phot_rp_mean_flux": np.float64,
            "phot_rp_mean_flux_over_error": np.float32,
            "phot_rp_mean_mag": np.float32,
            "phot_bp_rp_excess_factor": np.float32,
            "bp_rp": np.float32,
            "radial_velocity": np.float64,
            "radial_velocity_error": np.float64,
            "rv_nb_transits": "Int32",
            "rv_expected_sig_to_noise": np.float32,
            "rv_renormalised_gof": np.float32,
            "rv_chisq_pvalue": np.float32,
            "rvs_spec_sig_to_noise": np.float32,
            "grvs_mag": np.float32,
            "l": np.float64,
            "b": np.float64,
            "has_xp_continuous": bool,
            "has_xp_sampled": bool,
            "has_rvs": bool,
        }
        data = pd.read_csv(
            p, header=1, sep=",", skiprows=999, usecols=dtypes.keys(), dtype=dtypes
        )
        # write the data to a sqlite table
        data.to_sql("gaia_source", conn, if_exists="append", index=False)


# =================== indexing ===================
if do_indexing:
    print("=================== indexing ===================")
    # 4m57s
    print("Start doing allwise_best_neighbour_sourceid_designation indexing")
    c.execute(
        """CREATE INDEX allwise_best_neighbour_sourceid_designation ON allwise_best_neighbour (source_id, original_ext_source_id);"""
    )
    # 7m6s
    print("Start doing tmasspscxsc_best_neighbour_sourceid_designation indexing")
    c.execute(
        """CREATE INDEX tmasspscxsc_best_neighbour_sourceid_designation ON tmasspscxsc_best_neighbour (source_id, original_ext_source_id);"""
    )

    # other indexing seems to actually slow down whay I want to do
    # # 33m27s
    # print("Start doing gaia_source_sourceid indexing")
    # c.execute(
    #     """CREATE INDEX gaia_source_sourceid ON gaia_source (source_id, ruwe, ra, dec, pmra, pmdec, parallax, parallax_error, radial_velocity, radial_velocity_error, phot_g_mean_mag, bp_rp);"""
    # )
    # # 34m19s
    # c.execute(
    #     """CREATE INDEX gaia_source_has_xp_continuous ON gaia_source (has_xp_continuous, source_id, phot_g_mean_mag, phot_bp_mean_mag, bp_rp);"""
    # )
    # # 47m15s
    # print("Start doing gaia_source_randomindex indexing")
    # c.execute("""CREATE INDEX gaia_source_randomindex ON gaia_source (random_index);""")
    # # 57m17s
    # print("Start doing gaia_source_bprp indexing")
    # c.execute("""CREATE INDEX gaia_source_bprp ON gaia_source (bp_rp);""")
    # # 57m37s
    # print("Start doing gaia_source_parallax_over_error indexing")
    # c.execute(
    #     """CREATE INDEX gaia_source_parallax_over_error ON gaia_source (parallax_over_error);"""
    # )
    # # 117m17s
    # print("Start doing gaia_source_phot_g_mean_mag indexing")
    # c.execute(
    #     """CREATE INDEX gaia_source_phot_g_mean_mag ON gaia_source (phot_g_mean_mag);"""
    # )
    # # 58m4s
    # print("Start doing gaia_source_ruwe_ipd indexing")
    # c.execute("""CREATE INDEX gaia_source_ruwe ON gaia_source (ruwe, ipd_frac_multi_peak, ipd_gof_harmonic_amplitude);""")
    # # 43m51s
    # print("Start doing gaia_source_radial_velocity indexing")
    # c.execute(
    #     """CREATE INDEX gaia_source_radial_velocity ON gaia_source (radial_velocity);"""
    # )
    # # 33m32s
    # print("Start doing gaia_source_has_xp_sampled indexing")
    # c.execute(
    #     """CREATE INDEX gaia_source_has_xp_sampled ON gaia_source (has_xp_sampled);"""
    # )
    # # 32m57s
    # print("Start doing gaia_source_has_rvs indexing")
    # c.execute("""CREATE INDEX gaia_source_has_rvs ON gaia_source (has_rvs);""")


# # =================== indexing ===================
# if do_indexing:
#     print("=================== indexing ===================")
#     # 4m57s
#     print("Start doing allwise_best_neighbour_sourceid_designation indexing")
#     c.execute(
#         """CREATE INDEX allwise_best_neighbour_sourceid_designation ON allwise_best_neighbour (source_id, original_ext_source_id);"""
#     )
#     # 7m6s
#     print("Start doing tmasspscxsc_best_neighbour_sourceid_designation indexing")
#     c.execute(
#         """CREATE INDEX tmasspscxsc_best_neighbour_sourceid_designation ON tmasspscxsc_best_neighbour (source_id, original_ext_source_id);"""
#     )
#     # 33m27s
#     print("Start doing gaia_source_sourceid indexing")
#     c.execute(
#         """CREATE INDEX gaia_source_sourceid ON gaia_source (source_id, ruwe, ra, dec, pmra, pmdec, parallax, parallax_error, radial_velocity, radial_velocity_error, phot_g_mean_mag, bp_rp);"""
#     )
#     # 34m19s
#     c.execute(
#         """CREATE INDEX gaia_source_has_xp_continuous ON gaia_source (has_xp_continuous, source_id, phot_g_mean_mag, phot_bp_mean_mag, bp_rp);"""
#     )
#     # 47m15s
#     print("Start doing gaia_source_randomindex indexing")
#     c.execute("""CREATE INDEX gaia_source_randomindex ON gaia_source (random_index);""")
#     # 57m17s
#     print("Start doing gaia_source_bprp indexing")
#     c.execute("""CREATE INDEX gaia_source_bprp ON gaia_source (bp_rp);""")
#     # 57m37s
#     print("Start doing gaia_source_parallax_over_error indexing")
#     c.execute(
#         """CREATE INDEX gaia_source_parallax_over_error ON gaia_source (parallax_over_error);"""
#     )
#     # 117m17s
#     print("Start doing gaia_source_phot_g_mean_mag indexing")
#     c.execute(
#         """CREATE INDEX gaia_source_phot_g_mean_mag ON gaia_source (phot_g_mean_mag);"""
#     )
#     # 58m4s
#     print("Start doing gaia_source_ruwe indexing")
#     c.execute("""CREATE INDEX gaia_source_ruwe ON gaia_source (ruwe);""")
#     # we need both together anyway
#     # 66m26s
#     print("Start doing gaia_source_ipd indexing")
#     c.execute(
#         """CREATE INDEX gaia_source_ipd ON gaia_source (ipd_frac_multi_peak, ipd_gof_harmonic_amplitude);"""
#     )
#     # 43m51s
#     print("Start doing gaia_source_radial_velocity indexing")
#     c.execute(
#         """CREATE INDEX gaia_source_radial_velocity ON gaia_source (radial_velocity);"""
#     )
#     # 33m32s
#     print("Start doing gaia_source_has_xp_sampled indexing")
#     c.execute(
#         """CREATE INDEX gaia_source_has_xp_sampled ON gaia_source (has_xp_sampled);"""
#     )
#     # 32m57s
#     print("Start doing gaia_source_has_rvs indexing")
#     c.execute("""CREATE INDEX gaia_source_has_rvs ON gaia_source (has_rvs);""")
