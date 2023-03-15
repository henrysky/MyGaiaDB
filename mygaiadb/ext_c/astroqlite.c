/*
 * Missing ADQL functions for SQLite3
 */
#include <math.h>
#include <time.h>
#include <stdlib.h>
#include "sqlite3ext.h"

SQLITE_EXTENSION_INIT1

#ifdef _WIN32
#define EXPORT __declspec(dllexport)

#include <Python.h>
PyMODINIT_FUNC PyInit_astroqlite_c(void)
{
    return NULL;
};
#else
#define EXPORT
#endif

/* Constants */
#ifndef M_PI
#define M_PI 3.141592653589793238462643383279502884
#endif

static double radians(double x)
{
    return x * (M_PI / 180.0);
}

static double degrees(double x)
{
    return (x / M_PI) * 180.0;
}

static void piFunc(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    sqlite3_result_double(context, M_PI);
}

static void randFunc(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    double r;
    r = (double)rand() / RAND_MAX;
    sqlite3_result_double(context, r);
}

static double distance(double pt1, double pt2, double pt3, double pt4)
{
    double top;
    double bottom;
    double ang_sep;
    pt1 = radians(pt1);
    pt2 = radians(pt2);
    pt3 = radians(pt3);
    pt4 = radians(pt4);
    top = sqrt(pow(cos(pt4), 2) * pow(sin(pt3 - pt1), 2) + pow(cos(pt2) * sin(pt4) - sin(pt2) * cos(pt4) * cos(pt3 - pt1), 2));
    bottom = sin(pt2) * sin(pt4) + cos(pt2) * cos(pt4) * cos(pt3 - pt1);
    ang_sep = degrees(atan(top / bottom));
    return ang_sep;
}


static double gaia_healpix_index(double level, double source_id)
{
    double healpix;
    healpix = source_id / (pow(2, 35) * pow(4, 12 - level));
    return healpix;
}


/*
** Implementation of 1-argument SQL maths functions:
*/
static void math1Func(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int type0;
    double v0, ans;
    double (*x)(double);
    type0 = sqlite3_value_numeric_type(argv[0]);
    if (type0 != SQLITE_INTEGER && type0 != SQLITE_FLOAT)
    {
        return;
    }
    v0 = sqlite3_value_double(argv[0]);
    x = (double (*)(double))sqlite3_user_data(context);
    ans = x(v0);
    sqlite3_result_double(context, ans);
}

/*
** Implementation of 2-argument SQL maths functions:
*/
static void math2Func(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int type0, type1;
    double v0, v1, ans;
    double (*x)(double, double);
    type0 = sqlite3_value_numeric_type(argv[0]);
    type1 = sqlite3_value_numeric_type(argv[1]);
    if ((type0 != SQLITE_INTEGER && type0 != SQLITE_FLOAT) || (type1 != SQLITE_INTEGER && type1 != SQLITE_FLOAT))
    {
        return;
    }
    v0 = sqlite3_value_double(argv[0]);
    v1 = sqlite3_value_double(argv[1]);
    x = (double (*)(double, double))sqlite3_user_data(context);
    ans = x(v0, v1);
    sqlite3_result_double(context, ans);
}

/*
** Implementation of 4-argument SQL maths functions:
*/
static void math4Func(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int type0, type1, type2, type3;
    double v0, v1, v2, v3, ans;
    double (*x)(double, double, double, double);
    type0 = sqlite3_value_numeric_type(argv[0]);
    type1 = sqlite3_value_numeric_type(argv[1]);
    type2 = sqlite3_value_numeric_type(argv[2]);
    type3 = sqlite3_value_numeric_type(argv[3]);
    if ((type0 != SQLITE_INTEGER && type0 != SQLITE_FLOAT) || (type1 != SQLITE_INTEGER && type1 != SQLITE_FLOAT) || (type2 != SQLITE_INTEGER && type2 != SQLITE_FLOAT) || (type3 != SQLITE_INTEGER && type3 != SQLITE_FLOAT))
    {
        return;
    }
    v0 = sqlite3_value_double(argv[0]);
    v1 = sqlite3_value_double(argv[1]);
    v2 = sqlite3_value_double(argv[2]);
    v3 = sqlite3_value_double(argv[3]);
    x = (double (*)(double, double, double, double))sqlite3_user_data(context);
    ans = x(v0, v1, v2, v3);
    sqlite3_result_double(context, ans);
}

/*
 * Registers the extension.
 */
EXPORT int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi)
{
    SQLITE_EXTENSION_INIT2(pApi);
    srand(time(NULL));

    // https://www.ivoa.net/documents/ADQL/20180112/PR-ADQL-2.1-20180112.html#tth_sEc2.3
    sqlite3_create_function(db, "abs", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, fabs, math1Func, NULL, NULL);
    sqlite3_create_function(db, "ceiling", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, ceil, math1Func, NULL, NULL);
    sqlite3_create_function(db, "degrees", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, degrees, math1Func, NULL, NULL);
    sqlite3_create_function(db, "exp", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, exp, math1Func, NULL, NULL);
    sqlite3_create_function(db, "floor", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, ceil, math1Func, NULL, NULL);
    sqlite3_create_function(db, "log", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, log, math1Func, NULL, NULL);
    sqlite3_create_function(db, "log10", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, log10, math1Func, NULL, NULL);
    sqlite3_create_function(db, "mod", 2, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, fmod, math2Func, NULL, NULL);
    sqlite3_create_function(db, "pi", 0, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, 0, piFunc, NULL, NULL);
    sqlite3_create_function(db, "power", 2, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, pow, math2Func, NULL, NULL);
    sqlite3_create_function(db, "radians", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, radians, math1Func, NULL, NULL);
    sqlite3_create_function(db, "sqrt", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, sqrt, math1Func, NULL, NULL);
    sqlite3_create_function(db, "rand", 0, SQLITE_UTF8 | SQLITE_INNOCUOUS, 0, randFunc, NULL, NULL);
    sqlite3_create_function(db, "round", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, round, math1Func, NULL, NULL);
    sqlite3_create_function(db, "trunc", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, trunc, math1Func, NULL, NULL);
    sqlite3_create_function(db, "acos", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, acos, math1Func, NULL, NULL);
    sqlite3_create_function(db, "asin", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, asin, math1Func, NULL, NULL);
    sqlite3_create_function(db, "atan", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, atan, math1Func, NULL, NULL);
    sqlite3_create_function(db, "atan2", 2, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, atan2, math2Func, NULL, NULL);
    sqlite3_create_function(db, "cos", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, cos, math1Func, NULL, NULL);
    sqlite3_create_function(db, "sin", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, sin, math1Func, NULL, NULL);
    sqlite3_create_function(db, "tan", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, tan, math1Func, NULL, NULL);
    sqlite3_create_function(db, "cosh", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, cosh, math1Func, NULL, NULL);
    sqlite3_create_function(db, "sinh", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, sinh, math1Func, NULL, NULL);
    sqlite3_create_function(db, "tanh", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, tanh, math1Func, NULL, NULL);
    sqlite3_create_function(db, "acosh", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, acosh, math1Func, NULL, NULL);
    sqlite3_create_function(db, "asinh", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, asinh, math1Func, NULL, NULL);
    sqlite3_create_function(db, "atanh", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, atanh, math1Func, NULL, NULL);

    // ADQL Geometrical functions
    sqlite3_create_function(db, "distance", 4, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, distance, math4Func, NULL, NULL);

    // Gaia TAP+ ADQL functions
    sqlite3_create_function(db, "gaia_healpix_index", 2, SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC, gaia_healpix_index, math2Func, NULL, NULL);
    return SQLITE_OK;
}
