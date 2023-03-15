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
    assert c.execute("""SELECT COS(1.)""").fetchall() == np.cos(1.)
    assert c.execute("""SELECT SIN(1.)""").fetchall() == np.sin(1.)
    assert c.execute("""SELECT TAN(1.)""").fetchall() == np.tan(1.)

    # merak and dubhe for all amateur stargazer
    np.testing.assert_approx_equal(c.execute("""SELECT DISTANCE(165.458, 56.3825, 165.933, 61.7511)""").fetchall()[0][0], 5.37411, 4)

    # make sure random is actually random
    rand_1 = c.execute("""SELECT RAND()""").fetchall()
    rand_2 = c.execute("""SELECT RAND()""").fetchall()
    assert rand_1 != rand_2
