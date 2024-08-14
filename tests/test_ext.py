"""
Test SQL C extensions are working as expected compared to numpy and known values
"""

import sqlite3

import numpy as np
import numpy.testing as npt
import pytest
from mygaiadb.query import LocalGaiaSQL


@pytest.fixture(scope="module")
def sqlite3_conn():
    c = sqlite3.connect(":memory:")
    LocalGaiaSQL._load_sqlite3_ext(c)
    return c


def test_geometric_functions(sqlite3_conn):
    # test geometrical functions
    npt.assert_allclose(
        sqlite3_conn.execute("""SELECT COS(1.)""").fetchall()[0][0], np.cos(1.0)
    )
    npt.assert_allclose(
        sqlite3_conn.execute("""SELECT SIN(1.)""").fetchall()[0][0], np.sin(1.0)
    )
    npt.assert_allclose(
        sqlite3_conn.execute("""SELECT TAN(1.)""").fetchall()[0][0], np.tan(1.0)
    )


def test_random(sqlite3_conn):
    # make sure random is actually random
    rand_1 = sqlite3_conn.execute("""SELECT RAND()""").fetchall()
    rand_2 = sqlite3_conn.execute("""SELECT RAND()""").fetchall()
    with pytest.raises(AssertionError):
        npt.assert_allclose(rand_1, rand_2)


def test_distance(sqlite3_conn):
    # merak and dubhe for all amateur stargazer
    npt.assert_allclose(
        sqlite3_conn.execute(
            """SELECT DISTANCE(165.458, 56.3825, 165.933, 61.7511)"""
        ).fetchall()[0][0],
        5.3741116,
    )
    npt.assert_allclose(
        sqlite3_conn.execute(
            """SELECT DISTANCE(165.933, 61.7511, 165.458, 56.3825)"""
        ).fetchall()[0][0],
        5.3741116,
    )
    # known bad G.source_id = 4472832130942575872
    npt.assert_allclose(
        sqlite3_conn.execute(
            """SELECT DISTANCE(179., 10., 269.448503, 4.73942)"""
        ).fetchall()[0][0],
        89.6181177,
    )


def test_adql_func(sqlite3_conn):
    # examples on https://www.cosmos.esa.int/web/gaia-users/archive/writing-queries#adql_syntax_2
    npt.assert_allclose(
        sqlite3_conn.execute("""SELECT CBRT(27.)""").fetchall()[0][0], 3.0
    )

    # examples on https://www.cosmos.esa.int/web/gaia-users/archive/writing-queries#adql_syntax_1
    npt.assert_equal(
        sqlite3_conn.execute(
            """SELECT GAIA_HEALPIX_INDEX(4, 2060294888487267584)"""
        ).fetchall()[0][0],
        914,
    )
    npt.assert_equal(
        sqlite3_conn.execute(
            """SELECT GAIA_HEALPIX_INDEX(8, 539853444666465792)"""
        ).fetchall()[0][0],
        61374,
    )
    npt.assert_equal(
        sqlite3_conn.execute(
            """SELECT GAIA_HEALPIX_INDEX(8, 1678668421246394624)"""
        ).fetchall()[0][0],
        190842,
    )
    npt.assert_equal(
        sqlite3_conn.execute("""SELECT SIGN(-0.001)""").fetchall()[0][0], -1
    )
