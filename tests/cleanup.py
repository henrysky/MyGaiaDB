import os
import pathlib
import stat
import sys

if len(sys.argv) > 0:
    if sys.argv[1] == "i_swear_i_am_on_gh_action":  # make sure no accidential execution
        astro_data_path = pathlib.Path(os.getenv("MY_ASTRO_DATA")).expanduser()

        # delete every files ending with db
        for db_file in astro_data_path.rglob("*.db"):
            # undo read-only premission in case any
            db_file.chmod(stat.S_IWRITE)
            db_file.unlink()

        # delete every files ending with h5
        for h5_file in astro_data_path.rglob("*.h5"):
            # undo read-only premission in case any
            h5_file.chmod(stat.S_IWRITE)
            h5_file.unlink()
    else:
        print(
            "You probably don't want to run this script, are you sure you are on GH action?"
        )
