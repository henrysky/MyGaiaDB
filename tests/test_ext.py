import sqlite3
import numpy as np
from mygaiadb.query import LocalGaiaSQL


def test_c_exts():
    """
    Test SQL C extensions are working as expected compared to numpy and known values
    """
    c = sqlite3.connect(":memory:")
    LocalGaiaSQL._load_sqlite3_ext(c)
    # test geometrical functions
    np.testing.assert_approx_equal(c.execute("""SELECT COS(1.)""").fetchall()[0][0], np.cos(1.), 14)
    np.testing.assert_approx_equal(c.execute("""SELECT SIN(1.)""").fetchall()[0][0], np.sin(1.), 14)
    np.testing.assert_approx_equal(c.execute("""SELECT TAN(1.)""").fetchall()[0][0], np.tan(1.), 14)

    # make sure random is actually random
    rand_1 = c.execute("""SELECT RAND()""").fetchall()
    rand_2 = c.execute("""SELECT RAND()""").fetchall()
    assert rand_1 != rand_2

    # merak and dubhe for all amateur stargazer
    np.testing.assert_approx_equal(c.execute("""SELECT DISTANCE(165.458, 56.3825, 165.933, 61.7511)""").fetchall()[0][0], 5.37411, 4)

    # examples on https://www.cosmos.esa.int/web/gaia-users/archive/writing-queries#adql_syntax_1
    np.testing.assert_equal(c.execute("""SELECT GAIA_HEALPIX_INDEX(4, 2060294888487267584)""").fetchall()[0][0], 914)
    np.testing.assert_equal(c.execute("""SELECT SIGN(-0.001)""").fetchall()[0][0], -1)
