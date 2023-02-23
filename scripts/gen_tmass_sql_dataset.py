import sqlite3
from pathlib import Path
import os
import glob
import tqdm
import numpy as np
import pandas as pd
from mygaiadb import astro_data_path, tmass_sql_db_path

db_filename = os.path.join(tmass_sql_db_path)
Path(db_filename).touch()
conn = sqlite3.connect(db_filename)
c = conn.cursor()

# =================== 2MASS ===================
# this section will take 1 hour to run
schema_filename = os.path.join("scripts", "sql_schema", "twomass_psc_lite_schema")

with open(schema_filename) as f:
    lines = f.read().replace("\n", "")
c.execute(lines)

paths = os.path.join(astro_data_path, "2mass_mirror/psc_*.gz")
for p in tqdm.tqdm(glob.glob(paths)):
    dtypes = {
        "ra": np.float64,
        "dec": np.float64,
        "designation": str,
        "j_m": np.float32,
        "j_snr": np.float32,
        "h_m": np.float32,
        "h_snr": np.float32,
        "k_m": np.float32,
        "k_snr": np.float32,
        "ph_qual": str,
        "rd_flg": str,
        "bl_flg": str,
        "cc_flg": str,
        "ndet": str,
        "prox": np.float32,
    }
    data = pd.read_csv(
        p,
        header=None,
        sep="|",
        usecols=[0, 1, 5, 6, 9, 10, 13, 14, 17, 18, 19, 20, 21, 22, 23],
        names=dtypes.keys(),
        # dont allow white space in names since gaia best neightbour do not have white space
        converters={"designation": str.strip},
    )
    data = data.replace(r"\N", np.nan)
    data.astype(dtypes)

    # write the data to a sqlite table
    data.to_sql(f"twomass_psc", conn, if_exists="append", index=False)

# =================== indexing ===================
# 9m46s
print("Doing Indexing")
c.execute(
    """CREATE INDEX twomass_psc_designation_mags ON twomass_psc (designation, j_m, h_m, k_m);"""
)
