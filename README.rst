MyGaiaDB
===============

*Why share when you can have the whole Gaia database on your own locally?*

``MyGaiaDB`` is simple python package with a set of scripts to help you setup a local 
Gaia database (also 2MASS and ALLWISE) without the need of administrator privilege and is compatible to all 
major platforms (Linux, Mac and Windows) beacuse ``MyGaiaDB`` is **serverless** and requires Python 
only using ``sqlite`` as long as you have enough disk space.

This code is mainly to help myself managing data for my research project with Gaia DR3 XP spectra 
and not meant to fit research usage from every aspect of Gaia's 1 billion stars. The main motivation of this 
code is to make setuping local Gaia database with 2MASS and ALLWISE accessible to everyone. Possible use cases include 
but not limited to make very long complex query cross-matching to multiple databses that can take a long time 
to finish (where the online ESA `Gaia archive`_ has timeout limitation).

You are welcome to modify the code, make pull request to make this code to suit your and others need.

**This code probably will never be properly continuously tested since no way I can run this code with a few TB of gaia data on Github Actions**

(🚧Work in progress🏗️)

Installation
---------------

This code requires ``python >= 3.8`` with ``numpy``, ``pandas``, ``h5py``, ``astropy``, ``tqdm`` and ``sqlite3``. Some optional functionality requires ``galpy``, ``mwdust``. 

You can simply do ``python setup.py install`` or ``python setup.py develop`` to use this code.

You need to make sure you have at least ~8TB of free (preferably fast) disk space. First set an 
environment variable called **MY_ASTRO_DATA** which point to a folder that contains your 
astronomical data in general. Where under **MY_ASTRO_DATA**, there should be a folder that contains all 
gaia data and sdss data (i.e. **GAIA_TOOLS_DATA** environment variable from Jo Bovy's 
gaia_tools_ as well as **SDSS_LOCAL_SAS_MIRROR** environment 
variable from Jo Bovy's apogee_).

.. _apogee: https://github.com/jobovy/apogee
.. _gaia_tools: https://github.com/jobovy/gaia_tools

Official data links:

* Official Gaia data can be downloaded here: http://cdn.gea.esac.esa.int/Gaia/. 
* Official 2MASS data can be downloaded here: https://irsa.ipac.caltech.edu/2MASS/download/allsky/
* Official ALLWISE data can be downloaded here: https://irsa.ipac.caltech.edu/data/download/wise-allwise/

The **case sensitive** folder structure should look something like the following chart. If you already have the data but in a different structure and you do 
not want or can not move them, you can use symbolic link to create the required folder structure without duplicating files. 
For Linux, you can use ``ln -s {source-dir-or-file-path} {symbolic-dir-or-file-path}``. 
For Windows, you can use ``mklink {symbolic-file-path} {source-file-path}`` or ``mklink /D {symbolic-dir-path} {source-dir-path}``.

::

    $MY_ASTRO_DATA/
    ├── $GAIA_TOOLS_DATA/
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
    └── $SDSS_LOCAL_SAS_MIRROR/
        └── *we don't actually need sdss data here*

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

=======================================
gaia_source_lite                      
=======================================
source_id                             
random_index  
ra            
ra_error      
dec           
dec_error     
parallax      
parallax_error
parallax_over_error
pmra
pmra_error
pmdec
pmdec_error
ra_dec_corr
ra_parallax_corr
ra_pmra_corr
ra_pmdec_corr
dec_parallax_corr
dec_pmra_corr
dec_pmdec_corr
parallax_pmra_corr
parallax_pmdec_corr
pmra_pmdec_corr
astrometric_params_solved
nu_eff_used_in_astrometry
pseudocolour
pseudocolour_error
astrometric_matched_transits
ipd_gof_harmonic_amplitude
ipd_frac_multi_peak
ipd_frac_odd_win
ruwe
phot_g_mean_flux double
phot_g_mean_flux_over_error
phot_g_mean_mag
phot_bp_mean_flux
phot_bp_mean_flux_over_error
phot_bp_mean_mag
phot_rp_mean_flux
phot_rp_mean_flux_over_error
phot_rp_mean_mag
phot_bp_rp_excess_factor
bp_rp
radial_velocity_error
radial_velocity_error
rv_nb_transits
rv_expected_sig_to_noise 
rv_renormalised_gof
rv_chisq_pvalue
rvs_spec_sig_to_noise
grvs_mag
l
b
has_xp_continuous
has_xp_sampled
has_rvs
=======================================

SQL Query
------------

This query is too complex for `Gaia Archive`_, thus you will get timeout error but luckily you've got ``MyGaiaDB`` to do the job. 
The following example query from ``gaia_source_lite`` table, ``gaia_astrophysical_parameters`` table, 2MASS and ALLWISE table all at once.

.. _Gaia Archive: https://gea.esac.esa.int/archive/

..  code-block:: python

    from mygaiadb.query import LocalGaiaSQL

    # initialize a local Gaia SQL database instance
    local_db = LocalGaiaSQL()

    query = """
    SELECT G.source_id, G.ra, G.dec, G.pmra, G.pmdec, G.parallax, G.parallax_error, G.phot_g_mean_mag, GA.logg_gspspec,
    TM.j_m, AW.w1mpro
    FROM gaia_source as G
    INNER JOIN main.tmasspscxsc_best_neighbour as T on G.source_id = T.source_id
    INNER JOIN allwise_best_neighbour as W on W.source_id = T.source_id
    INNER JOIN tmass.twomass_psc as TM on TM.designation = T.original_ext_source_id
    INNER JOIN allwise.allwise as AW on AW.designation = W.original_ext_source_id
    INNER JOIN gastrophysical_params.gaia_astrophysical_parameters as GA on GA.source_id = G.source_id
    WHERE (G.has_xp_continuous = 1) AND (G.ruwe < 1.4) AND (G.ipd_frac_multi_peak <= 2) AND (G.ipd_gof_harmonic_amplitude<0.1) AND (GA.logg_gspspec < 3.0)
    """

    local_db.save_csv(query, "output.csv", chunchsize=50000, overwrite=True)

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
    FROM gaia_source as G
    LIMIT 100000
    """
    ra_conversion = QueryCallback(new_col_name="ra_rad", func=lambda ra: ra / 180 * np.pi)

    local_db.save_csv(query, "output.csv", chunchsize=50000, overwrite=True, callbacks=[ra_conversion])

We also have a few useful callbacks included by default to add columns like zero-point corrected parallax or extinction

..  code-block:: python

    from mygaiadb.query import ZeroPointCallback, DustCallback

    query = """
    SELECT G.source_id, G.ra, G.dec, G.parallax, G.phot_bp_mean_mag, G.nu_eff_used_in_astrometry, G.pseudocolour, G.astrometric_params_solved
    FROM gaia_source as G
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

Authors
-------------
-  | **Henry Leung** - henrysky_
   | University of Toronto
   | Contact Henry: henrysky.leung [at] utoronto.ca

License
-------------
This project is licensed under the MIT License - see the `LICENSE`_ file for details

.. _LICENSE: LICENSE
.. _henrysky: https://github.com/henrysky
