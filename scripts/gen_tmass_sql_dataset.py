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

# only the first part, not all actually
# https://irsa.ipac.caltech.edu/data/2MASS/docs/releases/allsky/doc/sec2_2a.html
tmass_allcol = [
    "ra",
    "dec",
    "err_maj",
    "err_min",
    "err_ang",
    "designation",
    # Primary Photometric Information
    "j_m",
    "j_cmsig",
    "j_msigcom",
    "j_snr",
    "h_m",
    "h_cmsig",
    "h_msigcom",
    "h_snr",
    "k_m",
    "k_cmsig",
    "k_msigcom",
    "k_snr",
    # Primary Source Quality Information
    "ph_qual",
    "rd_flg",
    "bl_flg",
    "cc_flg",
    "ndet",
    "prox",
    "pxpa",
    "pxcntr",
    "gal_contam",
    "mp_flg",
    # Additional Positional and Identification Information
    "pts_key/cntr",
    "hemis",
    "date",
    "scan",
    "glon",
    "glat",
    "x_scan",
    "jdate",
    # Additional Photometric Information
    "j_psfchi",
    "h_psfchi",
    "k_psfchi"
    "j_m_stdap",
    "j_msig_stdap",
    "h_m_stdap",
    "h_msig_stdap",
    "k_m_stdap",
    "k_msig_stdap",
    # Additional Source Quality Information
    "dist_edge_ns",
    "dist_edge_ew",
    "dist_edge_flg",
    "dup_src",
    "use_src",
    # Optical Source Association Information
    "a",
    "dist_opt",
    "phi_opt",
    "b_m_opt",
    "vr_m_opt",
    "nopt_mchs",
    # Cross-Index Information
    "ext_key",
    "scan_key",
    "coadd_key",
    "coadd"
]


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
        usecols=[tmass_allcol.index(i) for i in dtypes.keys()],
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
