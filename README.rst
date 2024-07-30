MyGaiaDB
===============

*Why share when you can have the entire Gaia database locally?*

``MyGaiaDB`` is a simple Python package that provides scripts to help you set up a local 
Gaia **DR3** database, along with local 2MASS, ALLWISE, and CATWISE databases, without needing 
administrative privileges. It's compatible with all major platforms (Linux, Mac, and Windows) 
because ``MyGaiaDB`` is **serverless** and only requires Python with ``sqlite``, as long as you have 
enough disk space.

This code was primarily developed to assist with my research project involving Gaia DR3 XP spectra. 
It is not intended to cover every aspect of research using Gaia's 1 billion stars. The main goal of 
this code is to make setting up a local Gaia database, along with 2MASS, ALLWISE, and CATWISE, 
accessible to everyone. Potential use cases include, but are not limited to, making complex and 
lengthy cross-matching queries with multiple databases, which can be time-consuming (especially 
given the online ESA Gaia archive's timeout limitations).


Feel free to modify the code and submit pull requests to enhance its functionality for yourself and others.

..

    Parts of this code are not continuously tested, as it is impractical to run this code with several terabytes of Gaia data on GitHub Actions.

.. contents:: **Table of Contents**
    :depth: 3

Installation and Dependencies
-------------------------------

This code requires ``python >= 3.8`` with ``numpy``, ``pandas``, ``h5py``, ``astropy`` and ``tqdm``. 
This code only needs the ``sqlite3`` library that comes bundled with your Python installation. 
For some optional functionalities,  ``galpy``, ``mwdust`` are required. Downloading functions will require ``wget``.

You can install the precompiled ``MyGaiaDB`` wheels by running `pip install mygaiadb`.

Alternatively, you can compile the code locally from the source. You'll need to add the folder containing ``sqlite3ext.h`` 
to your **INCLUDE** environment variable. For Conda users, the commands are:

- Windows Command Prompt: ``set INCLUDE=%CONDA_PREFIX%\Library\include;%INCLUDE%``
- Windows PowerShell: ``$env:INCLUDE="$env:CONDA_PREFIX\Library\include"``
- MacOS: ``export CFLAGS=-I$CONDA_PREFIX/include``
- Linux: No action needed usually

Then, you can run ``python -m pip install .`` to install the latest commits from GitHub or ```python -m pip install -e .``` to develop `MyGaiaDB` locally.

Folder Structure
-------------------

Ensure you have at least ~8TB of free disk space with fast **random read** speed for optimal query performance. First, set 
an environment variable called **MY_ASTRO_DATA** that points to a folder containing your astronomical data. To be compatible 
with other Python packages, there should be a folder called ``gaia_mirror`` under **MY_ASTRO_DATA** that contains all Gaia data (similar to the 
**GAIA_TOOLS_DATA** environment variable used by Jo Bovy's gaia_tools_).

.. _apogee: https://github.com/jobovy/apogee
.. _gaia_tools: https://github.com/jobovy/gaia_tools

If starting from scratch on a clean computer, set the **MY_ASTRO_DATA** environment variable, and 
``MyGaiaDB`` will populate the necessary files and folders. ``MyGaiaDB`` will use the ``~/.mygaiadb`` folder to save user-specific settings and tables.

If you already have the data on your computer but in a different directory structure and cannot move them, you can use symbolic links to create 
the required folder structure without duplicating files. For Linux and MacOS, use  ``ln -s {source-dir-or-file-path} {symbolic-dir-or-file-path}``. 
For Windows, use ``mklink {symbolic-file-path} {source-file-path}`` or ``mklink /D {symbolic-dir-path} {source-dir-path}``. 
The **case sensitive** folder structure should look something like the following chart:

::

    ~/
    ├── .mygaiadb

    $MY_ASTRO_DATA/
    ├── gaia_mirror/
    │   ├── Gaia/
    │   │   ├── gdr3/
    │   │   │   ├── Astrophysical_parameters/astrophysical_parameters/
    │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   ├── AstrophysicalParameters_000000-003111.csv.gz
    │   │   │   │   ├── ******
    │   │   │   │   └── AstrophysicalParameters_786097-786431.csv.gz
    │   │   │   ├── gaia_source/
    │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   ├── GaiaSource_000000-003111.csv.gz
    │   │   │   │   ├── ******
    │   │   │   │   └── GaiaSource_786097-786431.csv.gz
    │   │   │   ├── cross_match/
    │   │   │   │   ├── allwise_best_neighbour/
    │   │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   │   ├── allwiseBestNeighbour0001.csv.gz
    │   │   │   │   │   ├── ******
    │   │   │   │   │   └── allwiseBestNeighbour0033.csv.gz
    │   │   │   │   ├── tmasspscxsc_best_neighbour/
    │   │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   │   ├── tmasspscxscBestNeighbour0001.csv.gz
    │   │   │   │   │   ├── ******
    │   │   │   │   │   └── tmasspscxscBestNeighbour0047.csv.gz
    │   │   │   ├── Spectroscopy/
    │   │   │   │   ├── rvs_mean_spectrum/
    │   │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   │   ├── RvsMeanSpectrum_000000-003111.csv.gz
    │   │   │   │   │   ├── ******
    │   │   │   │   │   └── RvsMeanSpectrum_786097-786431.csv.gz
    │   │   │   │   ├── xp_continuous_mean_spectrum/
    │   │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   │   ├── XpContinuousMeanSpectrum_000000-003111.csv.gz
    │   │   │   │   │   ├── ******
    │   │   │   │   │   └── XpContinuousMeanSpectrum_786097-786431.csv.gz
    │   │   │   │   ├── xp_sampled_mean_spectrum/
    │   │   │   │   │   ├── _MD5SUM.txt
    │   │   │   │   │   ├── XpSampledMeanSpectrum_000000-003111.csv.gz
    │   │   │   │   │   ├── ******
    │   │   │   │   │   └── XpSampledMeanSpectrum_786097-786431.csv.gz
    ├── 2mass_mirror/
    │   ├── psc_aaa.gz
    │   ├── ******
    │   └── xsc_baa.gz
    ├── allwise_mirror/
    │   ├── wise-allwise-cat-part01.bz2
    │   ├── ******
    │   └── wise-allwise-cat-part48.bz2
    └── catwise_mirror/
        └── 2020/
            ├── 000/
            │   ├── 0000m016_opt1_20191208_213403_ab_v5_cat_b0.tbl.gz
            │   └── ******
            ├── 001/
            │   ├── 0015m016_opt1_20191209_054819_ab_v5_cat_b0.tbl.gz
            │   └── ******
            └── ******

Downloading Data
---------------------------

To download and populate data with ``MyGaiaDB``, you can do

..  code-block:: python

    from mygaiadb.data import download

    # for gaia_source
    download.download_gaia_source()
    # for gaia astrophysical_parameters
    download.download_gaia_astrophysical_parameters()
    # for 2mass best neightbour
    download.download_2mass_best_neightbour()
    # for allwise best neightbour
    download.download_allwise_best_neightbour()
    # for 2MASS
    download.download_2mass()
    # for allwise
    download.download_allwise()
    # for catwise
    download.download_catwise()
    # for xp continuous
    download.download_gaia_xp_continuous()
    # for xp sampled
    download.download_gaia_xp_sampled()    
    # for rvs spectra
    download.download_gaia_rvs()


Official data links:

* Official Gaia data can be accessed here: https://cdn.gea.esac.esa.int/Gaia/
* Official 2MASS data can be accessed here: https://irsa.ipac.caltech.edu/2MASS/download/allsky/
* Official ALLWISE data can be accessed here: https://irsa.ipac.caltech.edu/data/download/wise-allwise/
* Official CATWISE data can be accessed here: https://catwise.github.io/

Compiling Databases
---------------------
The following functions are used to compile the databases. **Each function only needs to be run once per computer where the data is stored**, 
and **each function will generate large-sized files**. If you are using a shared computing server, only one user needs to run these functions. 
Once the databases are compiled, share the **MY_ASTRO_DATA** folder path with other users. They can set their own **MY_ASTRO_DATA** environment 
variable to point to that folder. Multiple users on the same computers can access the SQL database simultaneously, as ``MyGaiaDB`` will set the 
databases to read-only mode before loading them, preventing accidental modifications.

..  code-block:: python

    from mygaiadb.data import compile

    # compile Gaia SQL dataset
    compile.compile_gaia_sql_db()
    # compile 2MASS SQL dataset
    compile.compile_tmass_sql_db()
    # compile ALLWISE SQL dataset
    compile.compile_allwise_sql_db()
    # compile CATWISE SQL dataset
    compile.compile_catwise_sql_db()

    # turn compressed XP coeffs files to h5, with options to save correlation matrix too
    # a large amount of disk space (~3TB) is required if save_correlation_matrix=True
    compile.compile_xp_continuous_h5(save_correlation_matrix=False)
    # compile all XP coeffs into a single h5, partitioned batches of stars by their HEALPix
    # with options to save correlation matrix too, BUT it requires yo to run compile_xp_continuous_h5(save_correlation_matrix=True) first
    # a large amount of disk space (~3TB) is required if save_correlation_matrix=True
    compile.compile_xp_continuous_allinone_h5(save_correlation_matrix=False)

SQL Databases Data Model
---------------------------

In ``MyGaiaDB``, the following databases are available if all have been compiled: 
``gaiadr3.gaia_source``, ``gaiadr3.allwise_best_neighbour``, ``gaiadr3.tmasspscxsc_best_neighbour``, 
``gaiadr3.astrophysical_parameters``, ``tmass.twomass_psc``, ``allwise.allwise`` and ``catwise.catwise``. 
A few utility functions are available to list tables and their columns. Below is a brief description of each table:

-   | ``gaiadr3.gaia_source``
    | This table mirrors ``gaia_source_lite`` on the `Gaia Archive`_ with the addition of ``grvs_mag`` column
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html
-   | ``gaiadr3.allwise_best_neighbour``
    | Identical to ``allwise_best_neighbour`` on `Gaia Archive`_
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_cross-matches/ssec_dm_allwise_best_neighbour.html
-   | ``gaiadr3.tmasspscxsc_best_neighbour``
    | Identical to ``tmass_psc_xsc_best_neighbour`` on `Gaia Archive`_
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_cross-matches/ssec_dm_tmass_psc_xsc_best_neighbour.html
-   | ``gaiadr3.astrophysical_parameters``
    | A simplified version of ``astrophysical_parameters`` on `Gaia Archive`_ with only essential columns retained
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_astrophysical_parameter_tables/ssec_dm_astrophysical_parameters.html
-   | ``tmass.twomass_psc``
    | A simplified version of 2MASS Point Source Catalog (PSC) with only essential columns retained
    | Official description: https://irsa.ipac.caltech.edu/2MASS/download/allsky/format_psc.html
-   | ``allwise.allwise``
    | A simplified version of ALLWISE source catalog with only essential columns retained
    | Official description: https://wise2.ipac.caltech.edu/docs/release/allwise/expsup/sec2_1a.html
-   | ``catwise.catwise``
    | A simplified version of CATWISE source catalog with only essential columns retained
    | Official description: https://irsa.ipac.caltech.edu/data/WISE/CatWISE/gator_docs/catwise_colDescriptions.html

You can use the ``list_all_tables()`` function to get a list of tables, excluding ``user_table``. For example:

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    # print a list of tables
    print(local_db.list_all_tables())


You can use ``get_table_column(table_name)`` to get a list of columns of a table which must be in the format of 
``{database_name}.{table_name}``, ``gaiadr3.gaia_source`` in this case

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    # print a list of columns of a table
    print(local_db.get_table_column("gaiadr3.gaia_source"))


If you want to manage and edit the databases with GUI, you can try to use `SQLiteStudio`_ or `DB Browser for SQLite`_.


SQL Query
------------

SQL query in ``MyGaiaDB`` is largely the same as `Gaia Archive`_. Although ``MyGaiaDB`` has implemented all mathematical functions in way aligned with `ADQL`_, 
``MyGaiaDB`` has only implemented a few advanced `ADQL`_ function like ``DISTANCE`` as well as `additional functions`_ on ESA Gaia TAP+. 

For example the following query which used a special function called ``DISTANCE`` defined in `ADQL`_

..  code-block:: sql

    SELECT DISTANCE(179., 10., G.ra, G.dec) as ang_sep
    FROM gaiadr3.gaia_source as G
    WHERE G.source_id = 4472832130942575872

returns 89.618118 on `Gaia Archive`_. And you can also use such query in the same manner as you would on `Gaia Archive`_

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()
    local_db.query("""
        SELECT DISTANCE(179., 10., G.ra, G.dec) as ang_sep
        FROM gaiadr3.gaia_source as G
        WHERE G.source_id = 4472832130942575872
    """)

and you will get the same result of 89.618118.

For example the following query which utilize conventional maths function to approximate uncertainty in Gaia G magnitude

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()
    # CDS equation for conversion: http://vizier.cds.unistra.fr/viz-bin/VizieR-n?-source=METAnot&catid=1350&notid=63&-out=text
    local_db.query("""
        SELECT sqrt(power(((2.5 / log(10)) * (1 / G.phot_g_mean_flux_over_error)), 2) + power(0.0027553202, 2)) as phot_g_mean_mag_error
        FROM gaiadr3.gaia_source as G
        WHERE G.source_id = 3158175803069175680
    """)

Another example is the following query that works on `Gaia Archive`_ will also work in ``MyGaiaDB`` to select the first 100 gaia sources with XP coefficients

..  code-block:: sql

    SELECT TOP 100 * 
    FROM gaiadr3.gaia_source as G 
    WHERE (G.has_xp_continuous = 'True')

To run this query in ``MyGaiaDB``, you can do the following and will get a pandas Dataframe back as the result

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()
    local_db.query("""SELECT TOP 100 * FROM gaiadr3.gaia_source as G  WHERE (G.has_xp_continuous = 'True')""")

The following example query is too complex for `Gaia Archive`_, thus you will get timeout error but luckily, you've got ``MyGaiaDB`` to do the job. 
The following example query from ``gaia_source`` table, ``astrophysical_parameters`` table, 2MASS and ALLWISE table all at once.
Moreover, ``MyGaiaDB`` set each dataset to **read-only** before loading it. If you want to edit the database afterward, you must set the 
appropriate permission manually each time you have used ``MyGaiaDB``.

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    query = """
    SELECT G.source_id, G.ra, G.dec, G.pmra, G.pmdec, G.parallax, G.parallax_error, G.phot_g_mean_mag, GA.logg_gspspec,
    TM.j_m, AW.w1mpro
    FROM gaiadr3.gaia_source as G
    INNER JOIN gaiadr3.astrophysical_parameters as GA on GA.source_id = G.source_id
    INNER JOIN gaiadr3.tmasspscxsc_best_neighbour as T on G.source_id = T.source_id
    INNER JOIN gaiadr3.allwise_best_neighbour as W on W.source_id = T.source_id
    INNER JOIN tmass.twomass_psc as TM on TM.designation = T.original_ext_source_id
    INNER JOIN allwise.allwise as AW on AW.designation = W.original_ext_source_id
    WHERE (G.has_xp_continuous = 1) AND (G.ruwe < 1.4) AND (G.ipd_frac_multi_peak <= 2) AND (G.ipd_gof_harmonic_amplitude<0.1) AND (GA.logg_gspspec < 3.0)
    """

    # take ~12 hours to complete
    local_db.save_csv(query, "output.csv", chunksize=50000, overwrite=True, comments=True)

As you can see for ``has_xp_continuous``, we can also use ``1`` to represent ``true`` which is used by Gaia archive but both are fine with ``MyGaiaDB``. 
The ``overwrite=True`` means the function will save the file even if the file with the same name already exists. The ``comments=True`` means the function will 
save the query as a comment in the csv file so you know how to reproduce the query result. To read the comments from the csv file, you can use the following code

..  code-block:: python

    from itertools import takewhile
    with open("output.csv", "r") as fobj:
        headiter = takewhile(lambda s: s.startswith("#"), fobj)
        header = list(headiter)
    print(" ".join(header).replace(" # ", "").replace("# ", ""))

``MyGaiaDB`` also has callbacks functionality called ``QueryCallback``, these callbacks can be used when you do query. For example, 
you can create a callbacks to convert ``ra`` in degree to ``ra_rad`` in radian. So your csv file in the end will have a new column 
called ``ra_rad``. Functions in ``QueryCallback`` must have arguments with **exact** column names in your query so ``MyGaiaDB`` knows 
which columns to use on the fly.

..  code-block:: python

    import numpy as np
    from mygaiadb.query import LocalGaiaSQL, QueryCallback

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    query = """
    SELECT G.source_id, G.ra, G.dec
    FROM gaiadr3.gaia_source as G
    LIMIT 100000
    """
    ra_conversion = QueryCallback(new_col_name="ra_rad", func=lambda ra: ra / 180 * np.pi)

    local_db.save_csv(query, "output.csv", chunksize=50000, overwrite=True, callbacks=[ra_conversion], comments=True)

We also have a few useful callbacks included by default to add columns like zero-point corrected parallax or extinction

..  code-block:: python

    from mygaiadb.query import ZeroPointCallback, DustCallback

    query = """
    SELECT G.source_id, G.ra, G.dec, G.parallax, G.phot_bp_mean_mag, G.nu_eff_used_in_astrometry, G.pseudocolour, G.astrometric_params_solved
    FROM gaiadr3.gaia_source as G
    LIMIT 100000
    """

    # adding zero-point corrected parallax using official Gaia DR3 parallax zero-point python package
    zp_callback = ZeroPointCallback(new_col_name="parallax_w_zp")
    # adding SFD E(B-V) in H band filter using mwdust python package
    dust_callback = DustCallback(new_col_name="sfd_ebv", filter="H", dustmap="SFD")

    local_db.save_csv(query, "output.csv", chunksize=50000, overwrite=True, callbacks=[zp_callback, dust_callback])

User tables
-------------

``MyGaiaDB`` support the use of user uploaded table. You can load your table first by ``pandas`` and then do

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL 
    local_db = LocalGaiaSQL()  
    local_db.upload_user_table(pd.DataFrame({"source_id": [5188146770731873152, 4611686018427432192, 5764607527332179584]}), tablename="my_table_1")

and then carry-on doing query with ``my_table_1`` cross-matching with other tables like 

..  code-block:: python

    local_db.query("""SELECT * FROM gaiadr3.gaia_source as G  INNER JOIN user_table.my_table_1 as MY on MY.source_id = G.source_id""")

You can check the list of your own user tables with column names by using ``list_user_tables()``

..  code-block:: python

    local_db.list_user_tables()

and you can remove a user table like ``my_table_1`` in this case by using ``remove_user_table()``

..  code-block:: python

    local_db.remove_user_table("my_table_1")

Gaia XP Spectroscopy Query
----------------------------

There can be use case where you want to run a function (e.g., a machine learning model) to a large batch of Gaia source ids with reasonable memory usage. 
You can use ``MyGaiaDB`` to do that in batch provided you have compiled a single h5 with ``mygaiadb.compile.compile_xp_continuous_allinone_h5()``

..  code-block:: python

    from mygaiadb.spec import yield_xp_coeffs

    for i in yield_xp_coeffs(a_very_long_source_id_array):
        coeffs, idx = i
        # XP coeffs of idx from the original a_very_long_source_id_array

    # alternatively if you also want coeffs error
    for i in yield_xp_coeffs(a_very_long_source_id_array, return_errors=True):
        coeffs, idx, coeffs_err = i  # unpack

    # alternatively if you want coeffs error and some other columns like bp_n_relevant_basesand rp_n_relevant_bases
    # ref: https://gea.esac.esa.int/archive/documentation/GDR3//Gaia_archive/chap_datamodel/sec_dm_spectroscopic_tables/ssec_dm_xp_summary.html
    for i in yield_xp_coeffs(a_very_long_source_id_array, return_errors=True, return_additional_columns=["bp_n_relevant_bases", "rp_n_relevant_bases"]):
        coeffs, idx, coeffs_err, bp_n_relevant_bases, rp_n_relevant_bases = i  # unpack

For example you want to infer ``M_H`` with your machine learning model on many XP spectra

..  code-block:: python

    from mygaiadb.spec import yield_xp_coeffs

    m_h = np.ones(len(a_very_long_source_id_array)) * np.nan
    for i in yield_xp_coeffs(a_very_long_source_id_array):
        coeffs, idx = i  # unpack
        m_h[idx] = your_ml_model(coeffs)

Author
-------------
-  | **Henry Leung** - henrysky_
   | University of Toronto
   | Contact Henry: henrysky.leung [at] utoronto.ca

This package is original developed for the paper `Towards an astronomical foundation model for stars with a Transformer-based model <https://ui.adsabs.harvard.edu/abs/2024MNRAS.527.1494L>`__.

License
-------------
This project is licensed under the MIT License - see the `LICENSE`_ file for details.

.. _Gaia Archive: https://gea.esac.esa.int/archive/
.. _ADQL: https://www.ivoa.net/documents/ADQL/
.. _additional functions: https://www.cosmos.esa.int/web/gaia-users/archive/writing-queries#adql_syntax_1
.. _SQLiteStudio: https://sqlitestudio.pl/
.. _DB Browser for SQLite: https://sqlitebrowser.org/
.. _LICENSE: LICENSE
.. _henrysky: https://github.com/henrysky
