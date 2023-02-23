import sqlite3
from pathlib import Path
import os
import glob
import tqdm
import numpy as np
import pandas as pd
from datetime import datetime
from mygaiadb import astro_data_path, gaia_astro_param_sql_db_path

# =================== settings ===================
do_gaia_source_table = True
do_indexing = True
# =================== settings ===================

db_filename = gaia_astro_param_sql_db_path
Path(db_filename).touch()
conn = sqlite3.connect(db_filename)
c = conn.cursor()

# =================== setup Gaia schema and first two tables ===================
# this section will take ~30 minutes to run
schema = "astrophysical_parameters_lite_schema"
schema_filename = os.path.join("scripts", "sql_schema", f"{schema}")

with open(schema_filename) as f:
    lines = f.read().replace("\n", "")
c.execute(lines)
# =================== populate gaia_source lite table ===================
# we have added "grvs_mag" to the table on top of gaia_source_lite on Gaia Archive
# will take ~11 hours to run
if do_gaia_source_table:
    path = os.path.join(astro_data_path, f"gaia_mirror/Gaia/gdr3/Astrophysical_parameters/astrophysical_parameters/*.csv.gz")
    glob_paths = glob.glob(path)

    for p in tqdm.tqdm(glob_paths):
        dtypes = {
            "source_id": np.int64,
            "classprob_dsc_combmod_quasar": np.float32,
            "classprob_dsc_combmod_galaxy": np.float32,
            "classprob_dsc_combmod_star": np.float32,
            "classprob_dsc_combmod_whitedwarf": np.float32,
            "classprob_dsc_combmod_binarystar": np.float32,
            "classprob_dsc_specmod_quasar": np.float32, 
            "classprob_dsc_specmod_galaxy": np.float32,
            "classprob_dsc_specmod_star": np.float32,
            "classprob_dsc_specmod_whitedwarf": np.float32,
            "classprob_dsc_specmod_binarystar": np.float32,
            "classprob_dsc_allosmod_quasar": np.float32,
            "classprob_dsc_allosmod_galaxy": np.float32,
            "classprob_dsc_allosmod_star": np.float32,
            "teff_gspphot": np.float32,
            "teff_gspphot_lower": np.float32,
            "teff_gspphot_upper": np.float32,
            "logg_gspphot": np.float32,
            "logg_gspphot_lower": np.float32,
            "logg_gspphot_upper": np.float32,
            "mh_gspphot": np.float32,
            "mh_gspphot_lower": np.float32,
            "mh_gspphot_upper": np.float32,
            "distance_gspphot": np.float32,
            "distance_gspphot_lower": np.float32,
            "distance_gspphot_upper": np.float32,
            "azero_gspphot": np.float32,
            "azero_gspphot_lower": np.float32,
            "azero_gspphot_upper": np.float32,
            "ag_gspphot": np.float32,
            "ag_gspphot_lower": np.float32,
            "ag_gspphot_upper": np.float32,
            "abp_gspphot": np.float32,
            "abp_gspphot_lower": np.float32,
            "abp_gspphot_upper": np.float32,
            "arp_gspphot": np.float32,
            "arp_gspphot_lower": np.float32,
            "arp_gspphot_upper": np.float32,
            "ebpminrp_gspphot": np.float32,
            "ebpminrp_gspphot_lower": np.float32,
            "ebpminrp_gspphot_upper": np.float32,
            "mg_gspphot": np.float32,
            "mg_gspphot_lower": np.float32,
            "mg_gspphot_upper": np.float32,
            "radius_gspphot": np.float32,
            "radius_gspphot_lower": np.float32,
            "radius_gspphot_upper": np.float32,
            "logposterior_gspphot": np.float32,
            "mcmcaccept_gspphot": np.float32,
            "libname_gspphot": str,
            "teff_gspspec": np.float32,
            "teff_gspspec_lower": np.float32,
            "teff_gspspec_upper": np.float32,
            "logg_gspspec": np.float32,
            "logg_gspspec_lower": np.float32,
            "logg_gspspec_upper": np.float32,
            "mh_gspspec": np.float32,
            "mh_gspspec_lower": np.float32,
            "mh_gspspec_upper": np.float32,
            "alphafe_gspspec": np.float32,
            "alphafe_gspspec_lower": np.float32,
            "alphafe_gspspec_upper": np.float32,
            "flags_gspspec": str,
            "activityindex_espcs": np.float32,
            "activityindex_espcs_uncertainty": np.float32,
            "activityindex_espcs_input": str
        }
        data = pd.read_csv(
            p, header=1, sep=",", skiprows=1540, usecols=dtypes.keys(), dtype=dtypes
        )
        # write the data to a sqlite table
        data.to_sql("gaia_astrophysical_parameters", conn, if_exists="append", index=False)

# =================== indexing ===================
if do_indexing:  # approx ~10Gb each indexing
    print("=================== indexing ===================")
    # 33m27s
    print("Start doing gaia_astrophysical_parameters_sourceid indexing")
    c.execute(
        """CREATE INDEX gaia_astrophysical_parameters_sourceid ON gaia_astrophysical_parameters (source_id);"""
    )