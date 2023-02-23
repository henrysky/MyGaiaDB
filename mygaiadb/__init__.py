import os

astro_data_path = os.getenv("MY_ASTRO_DATA")

gaia_sql_db_path = os.path.join(astro_data_path, "gaia_mirror/gaiadr3.db")
gaia_astro_param_sql_db_path = os.path.join(astro_data_path, "gaia_mirror/gaiadr3_astrophysical_params.db")
gaia_xp_coeff_h5_path = os.path.join(astro_data_path, "gaia_mirror/xp_continuous_mean_spectrum_allinone.h5")
tmass_sql_db_path = os.path.join(astro_data_path, "2mass_mirror/tmass.db")
allwise_sql_db_path = os.path.join(astro_data_path, "allwise_mirror/allwise.db")
