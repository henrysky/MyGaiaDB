import pathlib

from setuptools import Extension, setup

astroqlite_c_src = pathlib.Path("./src/mygaiadb/ext_c/astroqlite.c")

astroqlite_c = Extension(
    "mygaiadb.astroqlite_c",
    sources=[astroqlite_c_src.as_posix()],
    include_dirs=[pathlib.Path("./src/mygaiadb/ext_c/").as_posix()],
)

setup(
    ext_modules=[astroqlite_c],
)
