import sqlite3
from pathlib import Path
import os
import glob
import tqdm
import numpy as np
import pandas as pd
from mygaiadb import astro_data_path, allwise_sql_db_path

db_filename = allwise_sql_db_path
Path(db_filename).touch()
conn = sqlite3.connect(db_filename)
c = conn.cursor()

# =================== WISE ===================
# this section will take ~16 hours to run

schema_filename = os.path.join("scripts", "sql_schema", "allwise_lite_schema")

with open(schema_filename) as f:
    lines = f.read().replace("\n", "")
c.execute(lines)

paths = os.path.join(astro_data_path, "allwise_mirror/wise-allwise-cat-*.bz2")
for p in tqdm.tqdm(glob.glob(paths)):
    dtypes = {
        "designation": str,
        "ra": np.float64,
        "dec": np.float64,
        "w1mpro": np.float32,
        "w1snr": np.float32,
        "w2mpro": np.float32,
        "w2snr": np.float32,
        "w3mpro": np.float32,
        "w3snr": np.float32,
        "w4mpro": np.float32,
        "w4snr": np.float32,
        "cc_flags": str,
    }
    data = pd.read_csv(
        p,
        header=None,
        sep="|",
        usecols=[0, 1, 2, 16, 18, 20, 22, 24, 26, 28, 30, 57],
        names=dtypes.keys(),
        dtype=dtypes,
    )
    # write the data to a sqlite table
    data.to_sql(f"allwise", conn, if_exists="append", index=False)

# =================== indexing ===================
# 22m56s
print("Doing Indexing")
c.execute(
    """CREATE INDEX allwise_designation_mags ON allwise (designation, w1mpro, w2mpro, w3mpro, w4mpro);"""
)
