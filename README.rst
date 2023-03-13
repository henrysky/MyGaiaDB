MyGaiaDB
===============

*Why share when you can have the whole Gaia database on your own locally?*

``MyGaiaDB`` is simple python package with a set of scripts to help you setup a local 
Gaia **DR3** database (also local 2MASS and ALLWISE databases too) without the need of administrator privilege 
and is compatible to all major platforms (Linux, Mac and Windows) beacuse ``MyGaiaDB`` is **serverless** 
and requires Python only using ``sqlite`` as long as you have enough disk space.

This code is mainly to help myself managing data for my research project with Gaia DR3 XP spectra 
and not meant to fit research usage from every aspect of Gaia's 1 billion stars. The main motivation of this 
code is to make seting up local Gaia database with 2MASS and ALLWISE accessible to everyone. Possible use cases include 
but not limited to make very long complex query cross-matching to multiple databses that can take a long time 
to finish (where the online ESA `Gaia archive`_ has timeout limitation).

You are welcome to modify the code, make pull request to make this code to suit your and others need.

**This code probably will never be properly continuously tested since no way I can run this code with a few TB of gaia data on Github Actions**

(🚧Work in progress🏗️)

.. contents:: **Contents**
    :depth: 3

Installation and Dependencies
-------------------------------

This code requires ``python >= 3.8`` with ``numpy``, ``pandas``, ``h5py``, ``astropy``, ``tqdm``, ``beautifulsoup4`` and ``sqlite3``. 
Some optional functionalities requires ``galpy``, ``mwdust``. Downloading function requires ``wget``.

You can simply do ``python -m pip install .`` to use or ``python -m pip install -e .`` to develop ``MyGaiaDB`` locally.

Folder Structure
-------------------

You need to make sure you have at least ~8TB of free disk space with fast **random read** speed for optimal query performance. 
First set an environment variable called **MY_ASTRO_DATA** which point to a folder that (will) contains your 
astronomical data in general. To be compatiable with other python package, under **MY_ASTRO_DATA** there should be a folder called ``gaia_mirror`` that contains all 
gaia data (i.e. **GAIA_TOOLS_DATA** environment variable from Jo Bovy's gaia_tools_).

.. _apogee: https://github.com/jobovy/apogee
.. _gaia_tools: https://github.com/jobovy/gaia_tools

If you start from stratch, you only need to set **MY_ASTRO_DATA** environment variable and ``MyGaiaDB`` will populate the files and folders. 
``MyGaiaDB`` will use ``~/.mygaiadb`` folder to save user specific settings an tables.

If you already have the data on your computer but in a  different directory structure and you do  not want or can not move them, 
you can use symbolic link to create the required folder structure without 
duplicating files. For Linux and MacOS, you can use ``ln -s {source-dir-or-file-path} {symbolic-dir-or-file-path}``. 
For Windows, you can use ``mklink {symbolic-file-path} {source-file-path}`` or ``mklink /D {symbolic-dir-path} {source-dir-path}``. 
The **case sensitive** folder structure should look something like the following chart. 

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
    └── allwise_mirror/
        ├── wise-allwise-cat-part01.bz2
        ├── ******
        └── wise-allwise-cat-part48.bz2


Downloading Data
---------------------------

Official data links:

* Official Gaia data can be accessed here: https://cdn.gea.esac.esa.int/Gaia/
* Official 2MASS data can be accessed here: https://irsa.ipac.caltech.edu/2MASS/download/allsky/
* Official ALLWISE data can be accessed here: https://irsa.ipac.caltech.edu/data/download/wise-allwise/

To download ``gaia_source``, 

Post installation scripts
--------------------------------
Here are some post installation scripts (each only need to be ran once on each computer you store the data). 
**Each sctipt will generate large sized file(s)**. You can simply run ``python scripts/{name-of-the-script}.py``. 
Moreover if you are using a shared computing server, only one user need to run the scripts and share **MY_ASTRO_DATA** folder path to other user so
they can setup their own enviroment variable **MY_ASTRO_DATA** to that folder too. Multiple users can use the SQL database at the same time as long as you have set permission 
correctly so no accidential delete or modification.

-   | `scripts/gen_gaia_sql_dataset.py`_
    | Script to generate ``gaia_source_lite`` table (same layout as ``gaia_source_lite`` on `Gaia Archive`_ with addition of ``grvs_mag``) along with 2MASS and ALLWISE best neightbour table into a singele SQL database
    | This script will also do indexing on commonly used column. The whole script will take ~20 hours to run.
-   | `scripts/gen_gaia_astro_param_sql_dataset.py`_
    | Script to generate a stripped down version of ``astrophysical_parameters`` table into a singele SQL database
    | This script will also do indexing on commonly used column. The whole script will take ~12 hours to run.
-   | `scripts/gen_allwise_sql_dataset.py`_
    | Script to generate a stripped down version of ALLWISE photometry table into a singele SQL database
    | This script will also do indexing on commonly used column. The whole script will take ~16 hours to run.
-   | `scripts/gen_tmass_sql_dataset.py`_
    | Script to generate a stripped down version of 2MASS photometry table into a singele SQL database
    | This script will also do indexing on commonly used column. The whole script will take ~1 hours to run.
-   | `scripts/gen_spectra_h5.py`_
    | Script to turn all spectra files into h5 file format
    | This script will also do indexing on commonly used column. The whole script will take ~4 hours to run.
-   | `scripts/gen_xp_coeffs_h5.py`_
    | Script to generate a single h5 file while preserving the original healpix level 8 structure without correlation matrix
    | This script will also do indexing on commonly used column. The whole script will take ~1 hours to run.

.. _scripts/gen_gaia_sql_dataset.py: scripts/gen_gaia_sql_dataset.py
.. _scripts/gen_gaia_astro_param_sql_dataset.py: scripts/gen_gaia_astro_param_sql_dataset.py
.. _scripts/gen_allwise_sql_dataset.py: scripts/gen_allwise_sql_dataset.py
.. _scripts/gen_tmass_sql_dataset.py: scripts/gen_tmass_sql_dataset.py
.. _scripts/gen_spectra_h5.py: scripts/gen_spectra_h5.py
.. _scripts/gen_xp_coeffs_h5.py: scripts/gen_xp_coeffs_h5.py

SQL Databases Data Model
---------------------------

Currently for Gaia DR3 in ``MyGaiaDB``, these tales are avaliable if you have compiled all databases: 
``gaiadr3.gaia_source``, ``gaiadr3.allwise_best_neighbour``, ``gaiadr3.tmasspscxsc_best_neighbour``, 
``gaiadr3.astrophysical_parameters``, ``tmass.twomass_psc``, ``allwise.allwise``. But there are a few 
utility functions to see list of tables and table's columns. Brief description of the tables are as following:

-   | ``gaiadr3.gaia_source``
    | This table mimics ``gaia_source_lite`` on `Gaia Archive`_ with addition of ``grvs_mag`` columns
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html
-   | ``gaiadr3.allwise_best_neighbour``
    | This table is identical to ``allwise_best_neighbour`` on `Gaia Archive`_
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_cross-matches/ssec_dm_allwise_best_neighbour.html
-   | ``gaiadr3.tmasspscxsc_best_neighbour``
    | This table is identical to ``tmass_psc_xsc_best_neighbour`` on `Gaia Archive`_
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_cross-matches/ssec_dm_tmass_psc_xsc_best_neighbour.html
-   | ``gaiadr3.astrophysical_parameters``
    | This table is a lite version of ``astrophysical_parameters`` on `Gaia Archive`_ with only essential useful columns are kept
    | Official description: https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_astrophysical_parameter_tables/ssec_dm_astrophysical_parameters.html
-   | ``tmass.twomass_psc``
    | This table is a lite version of 2MASS Point Source Catalog (PSC) with only essential useful columns are kept
    | Official description: https://irsa.ipac.caltech.edu/2MASS/download/allsky/format_psc.html
-   | ``allwise.allwise``
    | This table is a lite version of ALLWISE source catalog with only essential useful columns are kept
    | Official description: https://wise2.ipac.caltech.edu/docs/release/allwise/expsup/sec2_1a.html

You can use ``get_all_tables()`` to get a list of tables. do 

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    # print a list of tables
    print(local_db.get_all_tables())


You can use ``get_table_cols(table_name)`` To get a list of columns of a table which must be in the format of 
``{database_name}.{table_name}``, ``gaiadr3.gaia_source`` in this case

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    # print a list of columns of a table
    print(local_db.get_table_cols("gaiadr3.gaia_source"))


If you want to manage and edit the databases with GUI, you can try to use `SQLiteStudio`_  or `DB Browser for SQLite`_.

SQL Query
------------

SQL query in ``MyGaiaDB`` is largely the same as `Gaia Archive`_ except ``MyGaiaDB`` does not have advanced SQL functions 
like geometrical functions. For example the following query that works on `Gaia Archive`_ will also work in ``MyGaiaDB`` to 
select the first 100 gaia sources with XP coefficients

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

The following example query is too complex for `Gaia Archive`_, thus you will get timeout error but luckily you've got ``MyGaiaDB`` to do the job. 
The following example query from ``gaia_source_lite`` table, ``gaia_astrophysical_parameters`` table, 2MASS and ALLWISE table all at once.
Moreover, ``MyGaiaDB`` set each dataset to **read-only** before loading it. If you want to edit the database afterward, you have to set the 
appropiate premission manually each time you have used ``MyGaiaDB``.

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    query = """
    SELECT G.source_id, G.ra, G.dec, G.pmra, G.pmdec, G.parallax, G.parallax_error, G.phot_g_mean_mag, GA.logg_gspspec,
    TM.j_m, AW.w1mpro
    FROM gaiadr3.gaia_source as G
    INNER JOIN gaiadr3.gaia_astrophysical_parameters as GA on GA.source_id = G.source_id
    INNER JOIN gaiadr3.tmasspscxsc_best_neighbour as T on G.source_id = T.source_id
    INNER JOIN gaiadr3.allwise_best_neighbour as W on W.source_id = T.source_id
    INNER JOIN tmass.twomass_psc as TM on TM.designation = T.original_ext_source_id
    INNER JOIN allwise.allwise as AW on AW.designation = W.original_ext_source_id
    WHERE (G.has_xp_continuous = 1) AND (G.ruwe < 1.4) AND (G.ipd_frac_multi_peak <= 2) AND (G.ipd_gof_harmonic_amplitude<0.1) AND (GA.logg_gspspec < 3.0)
    """

    # take ~12 hours to complete
    local_db.save_csv(query, "output.csv", chunchsize=50000, overwrite=True)

As you can see for ``has_xp_continuous``, we use ``1`` to represent ``TRUE`` which is different from Gaia archive.

``MyGaiaDB`` also has callbacks funcationality called ``QueryCallback``, these callbacks can be used when you do query. For example, 
you can create a callbacks to convert ``ra`` in degree to `ra_rad` in radian. So your csv file in the end will have a new column 
called ``ra_rad``. Functions in ``QueryCallback`` must have argeuments with **exact** column names in your query so ``MyGaiaDB`` knows 
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

    local_db.save_csv(query, "output.csv", chunchsize=50000, overwrite=True, callbacks=[ra_conversion])

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

    local_db.save_csv(query, "output.csv", chunchsize=50000, overwrite=True, callbacks=[zp_callback, dust_callback])

Spectroscopy Query
--------------------

There can be use case where you want to run a function (e.g. a machine learning model) to a large batch of source_id with reasonable memory usage. 
You can use ``MyGaiaDB`` to do that too in batch

..  code-block:: python

    from mygaiadb.spec import yield_xp_coeffs

    for i in yield_xp_coeffs(a_very_long_source_id_array):
        coeffs, idx = i
        # XP coeffs of idx from the original a_very_long_source_id_array

For example you want to infer ``M_H`` with your machine learning model

..  code-block:: python

    from mygaiadb.spec import yield_xp_coeffs

    m_h = np.ones(len(a_very_long_source_id_array)) * -9999.
    for i in yield_xp_coeffs(a_very_long_source_id_array):
        coeffs, idx = i
        m_h[idx] = your_ml_model(coeffs)

Author
-------------
-  | **Henry Leung** - henrysky_
   | University of Toronto
   | Contact Henry: henrysky.leung [at] utoronto.ca

License
-------------
This project is licensed under the MIT License - see the `LICENSE`_ file for details

.. _Gaia Archive: https://gea.esac.esa.int/archive/
.. _SQLiteStudio: https://sqlitestudio.pl/
.. _DB Browser for SQLite: https://sqlitebrowser.org/
.. _LICENSE: LICENSE
.. _henrysky: https://github.com/henrysky
