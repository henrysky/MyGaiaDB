import os
import sys
import stat
import pathlib

if len(sys.argv) > 0:
    if sys.argv[1] == "i_swear_i_am_on_gh_action":  # make sure no accidential execution
        astro_data_path = os.getenv("MY_ASTRO_DATA")
        astro_data_path = pathlib.Path(astro_data_path).expanduser()

        # delete every files ending with db
        for i in list(astro_data_path.rglob("*.db")):
            # undo read-only premission in case any
            i.chmod(stat.S_IWRITE)
            i.gaia_sql_db_path.unlink()

        # delete every files ending with h5
        for i in list(astro_data_path.rglob("*.h5")):
            # undo read-only premission in case any
            i.chmod(stat.S_IWRITE)
            i.gaia_sql_db_path.unlink()
