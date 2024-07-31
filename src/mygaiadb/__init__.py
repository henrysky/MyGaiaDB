import os
import pathlib
from importlib.metadata import version
import importlib.util

version = __version__ = version("MyGaiaDB")

mygaiadb_path = pathlib.Path(importlib.util.find_spec("mygaiadb").origin).parent

# make sure (shared) database folder exists
astro_data_path = os.getenv("MY_ASTRO_DATA")
if astro_data_path is None:
    raise EnvironmentError("Please specify an environment variable - MY_ASTRO_DATA")
else:
    astro_data_path = pathlib.Path(astro_data_path).expanduser()
    if not astro_data_path.exists():
        astro_data_path.mkdir()

# make sure user-specific database folder exists
mygaiadb_folder = pathlib.Path.home().joinpath(".mygaiadb")
mygaiadb_default_db = mygaiadb_folder.joinpath("mygaiadb.db")
mygaiadb_usertable_db = mygaiadb_folder.joinpath("user_table.db")
if not mygaiadb_folder.exists():
    mygaiadb_folder.mkdir()
mygaiadb_default_db.touch()
mygaiadb_usertable_db.touch()

gaia_sql_db_path = astro_data_path.joinpath("gaia_mirror", "gaiadr3.db")
gaia_astro_param_sql_db_path = astro_data_path.joinpath(
    "gaia_mirror", "gaiadr3_astrophysical_params.db"
)
gaia_xp_coeff_h5_path = astro_data_path.joinpath(
    "gaia_mirror", "xp_continuous_mean_spectrum_allinone.h5"
)
tmass_sql_db_path = astro_data_path.joinpath("2mass_mirror", "tmass.db")
allwise_sql_db_path = astro_data_path.joinpath("allwise_mirror", "allwise.db")
catwise_sql_db_path = astro_data_path.joinpath("catwise_mirror", "catwise.db")
