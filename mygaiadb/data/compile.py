import sqlite3
from pathlib import Path
import os
import glob
import tqdm
import h5py
import numpy as np
import pandas as pd
from . import (
    _GAIA_DR3_GAIASOURCE_PARENT,
    _GAIA_DR3_ASTROPHYS_PARENT,
    _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT,
    _GAIA_DR3_2MASS_NEIGHBOUR_PARENT,
    _2MASS_PARENT,
    _ALLWISE_PARENT
)
from .. import astro_data_path, gaia_sql_db_path, tmass_sql_db_path, allwise_sql_db_path, gaia_xp_coeff_h5_path


def compile_xp_continuous_allinone_h5():
    base_path = astro_data_path.joinpath(
        "gaia_mirror",
        "Gaia",
        "gdr3",
        "Spectroscopy",
        "xp_continuous_mean_spectrum",
    )
    file_paths = list(base_path.glob("*.h5"))

    if len(file_paths) == 0:
        raise FileNotFoundError(f"Gaia data does not exist at {base_path}")

    file_names = [x.name for x in file_paths]
    healpix_8_min = [
        int(file[file.find("_") + 1 : file.rfind("-")]) for file in file_names
    ]
    healpix_8_max = [
        int(file[file.rfind("-") + 1 : file.rfind(".h5")]) for file in file_names
    ]

    if gaia_xp_coeff_h5_path.exists():
        raise FileExistsError(f"File already existed in {gaia_xp_coeff_h5_path}")

    h5f = h5py.File(gaia_xp_coeff_h5_path, "w")
    for i in tqdm.tqdm(range(len(file_paths))):
        temp_h5_data = h5py.File(file_paths[i].as_posix(), "r")
        gp = h5f.create_group(f"{healpix_8_min[i]}-{healpix_8_max[i]}")
        gp.create_dataset("source_id", data=temp_h5_data["source_id"][()])
        gp.create_dataset("solution_id", data=temp_h5_data["solution_id"][()])
        gp.create_dataset(
            "bp_basis_function_id", data=temp_h5_data["bp_basis_function_id"][()]
        )
        gp.create_dataset(
            "bp_degrees_of_freedom", data=temp_h5_data["bp_degrees_of_freedom"][()]
        )
        gp.create_dataset("bp_n_parameters", data=temp_h5_data["bp_n_parameters"][()])
        gp.create_dataset(
            "bp_n_measurements", data=temp_h5_data["bp_n_measurements"][()]
        )
        gp.create_dataset(
            "bp_n_rejected_measurements",
            data=temp_h5_data["bp_n_rejected_measurements"][()],
        )
        gp.create_dataset(
            "bp_standard_deviation", data=temp_h5_data["bp_standard_deviation"][()]
        )
        gp.create_dataset("bp_chi_squared", data=temp_h5_data["bp_chi_squared"][()])
        gp.create_dataset(
            "bp_coefficient_errors", data=temp_h5_data["bp_coefficient_errors"][()]
        )
        gp.create_dataset("bp_coefficients", data=temp_h5_data["bp_coefficients"][()])
        gp.create_dataset(
            "bp_n_relevant_bases", data=temp_h5_data["bp_n_relevant_bases"][()]
        )
        gp.create_dataset(
            "bp_relative_shrinking", data=temp_h5_data["bp_relative_shrinking"][()]
        )

        gp.create_dataset(
            "rp_basis_function_id", data=temp_h5_data["rp_basis_function_id"][()]
        )
        gp.create_dataset(
            "rp_degrees_of_freedom", data=temp_h5_data["rp_degrees_of_freedom"][()]
        )
        gp.create_dataset("rp_n_parameters", data=temp_h5_data["rp_n_parameters"][()])
        gp.create_dataset(
            "rp_n_measurements", data=temp_h5_data["rp_n_measurements"][()]
        )
        gp.create_dataset(
            "rp_n_rejected_measurements",
            data=temp_h5_data["rp_n_rejected_measurements"][()],
        )
        gp.create_dataset(
            "rp_standard_deviation", data=temp_h5_data["rp_standard_deviation"][()]
        )
        gp.create_dataset("rp_chi_squared", data=temp_h5_data["rp_chi_squared"][()])
        gp.create_dataset(
            "rp_coefficient_errors", data=temp_h5_data["rp_coefficient_errors"][()]
        )
        gp.create_dataset("rp_coefficients", data=temp_h5_data["rp_coefficients"][()])
        gp.create_dataset(
            "rp_n_relevant_bases", data=temp_h5_data["rp_n_relevant_bases"][()]
        )
        gp.create_dataset(
            "rp_relative_shrinking", data=temp_h5_data["rp_relative_shrinking"][()]
        )
        temp_h5_data.close()
    h5f.close()


def compile_xp_continuous_h5(save_correlation_matrix=False):
    for i_path in tqdm.tqdm(
        list(astro_data_path.joinpath(
            "Gaia", "gdr3", "Spectroscopy", "xp_continuous_mean_spectrum"
        ).glob("*.csv.gz")),
        desc="XP coeffs",
    ):
        file_path_f = ascii.read(i_path)

        bp_basis_function_id = np.asarray(
            file_path_f["bp_basis_function_id"], dtype=np.int16
        )
        bp_degrees_of_freedom = np.asarray(
            file_path_f["bp_degrees_of_freedom"], dtype=np.int16
        )
        bp_n_parameters = np.asarray(file_path_f["bp_n_parameters"], dtype=np.int8)
        bp_n_measurements = np.asarray(file_path_f["bp_n_measurements"], dtype=np.int16)
        bp_n_rejected_measurements = np.asarray(
            file_path_f["bp_n_rejected_measurements"], dtype=np.int16
        )
        bp_standard_deviation = np.asarray(
            file_path_f["bp_standard_deviation"], dtype=np.float32
        )
        bp_chi_squared = np.asarray(file_path_f["bp_chi_squared"], dtype=np.float32)
        if save_correlation_matrix:
            bp_coefficient_correlations = np.stack(
                file_path_f["bp_coefficient_correlations"]
            )
        bp_coefficient_errors = np.stack(file_path_f["bp_coefficient_errors"])
        bp_coefficients = np.stack(file_path_f["bp_coefficients"])
        bp_n_relevant_bases = np.asarray(
            file_path_f["bp_n_relevant_bases"], dtype=np.int16
        )
        bp_relative_shrinking = np.asarray(
            file_path_f["bp_relative_shrinking"], dtype=np.float32
        )

        rp_basis_function_id = np.asarray(
            file_path_f["rp_basis_function_id"], dtype=np.int16
        )
        rp_degrees_of_freedom = np.asarray(
            file_path_f["rp_degrees_of_freedom"], dtype=np.int16
        )
        rp_n_parameters = np.asarray(file_path_f["rp_n_parameters"], dtype=np.int8)
        rp_n_measurements = np.asarray(file_path_f["rp_n_measurements"], dtype=np.int16)
        rp_n_rejected_measurements = np.asarray(
            file_path_f["rp_n_rejected_measurements"], dtype=np.int16
        )
        rp_standard_deviation = np.asarray(
            file_path_f["rp_standard_deviation"], dtype=np.float32
        )
        rp_chi_squared = np.asarray(file_path_f["rp_chi_squared"], dtype=np.float32)
        if save_correlation_matrix:
            rp_coefficient_correlations = np.stack(
                file_path_f["rp_coefficient_correlations"]
            )
        rp_coefficient_errors = np.stack(file_path_f["rp_coefficient_errors"])
        rp_coefficients = np.stack(file_path_f["rp_coefficients"])
        rp_n_relevant_bases = np.asarray(
            file_path_f["rp_n_relevant_bases"], dtype=np.int16
        )
        rp_relative_shrinking = np.asarray(
            file_path_f["rp_relative_shrinking"], dtype=np.float32
        )

        file_names_wo_ext = i_path.name[:-7]
        with h5py.File(
            f"./xp_continuous_mean_spectrum/{file_names_wo_ext}.h5", "w"
        ) as h5f:
            h5f.create_dataset("source_id", data=file_path_f["source_id"].data)
            h5f.create_dataset("solution_id", data=file_path_f["solution_id"].data)
            h5f.create_dataset("bp_basis_function_id ", data=bp_basis_function_id)
            h5f.create_dataset("bp_degrees_of_freedom ", data=bp_degrees_of_freedom)
            h5f.create_dataset("bp_n_parameters ", data=bp_n_parameters)
            h5f.create_dataset("bp_n_measurements ", data=bp_n_measurements)
            h5f.create_dataset(
                "bp_n_rejected_measurements ", data=bp_n_rejected_measurements
            )
            h5f.create_dataset("bp_standard_deviation ", data=bp_standard_deviation)
            h5f.create_dataset("bp_chi_squared ", data=bp_chi_squared)
            if save_correlation_matrix:
                h5f.create_dataset(
                    "bp_coefficient_correlations ", data=bp_coefficient_correlations
                )
            h5f.create_dataset("bp_coefficient_errors", data=bp_coefficient_errors)
            h5f.create_dataset("bp_coefficients", data=bp_coefficients)
            h5f.create_dataset("bp_n_relevant_bases", data=bp_n_relevant_bases)
            h5f.create_dataset("bp_relative_shrinking", data=bp_relative_shrinking)

            h5f.create_dataset("rp_basis_function_id ", data=rp_basis_function_id)
            h5f.create_dataset("rp_degrees_of_freedom ", data=rp_degrees_of_freedom)
            h5f.create_dataset("rp_n_parameters ", data=rp_n_parameters)
            h5f.create_dataset("rp_n_measurements ", data=rp_n_measurements)
            h5f.create_dataset(
                "rp_n_rejected_measurements ", data=rp_n_rejected_measurements
            )
            h5f.create_dataset("rp_standard_deviation ", data=rp_standard_deviation)
            h5f.create_dataset("rp_chi_squared ", data=rp_chi_squared)
            if save_correlation_matrix:
                h5f.create_dataset(
                    "rp_coefficient_correlations ", data=rp_coefficient_correlations
                )
            h5f.create_dataset("rp_coefficient_errors", data=rp_coefficient_errors)
            h5f.create_dataset("rp_coefficients", data=rp_coefficients)
            h5f.create_dataset("rp_n_relevant_bases", data=rp_n_relevant_bases)
            h5f.create_dataset("rp_relative_shrinking", data=rp_relative_shrinking)
            h5f.close()


def compile_rvs_h5():
    for i_path in tqdm.tqdm(
        list(astro_data_path.joinpath("Gaia/gdr3/Spectroscopy/rvs_mean_spectrum/").glob(
            "*.csv.gz"
        )),
        desc="RVS spec",
    ):
        file_path_f = ascii.read(i_path)
        flux = np.vstack(file_path_f["flux"])
        flux_error = np.vstack(file_path_f["flux_error"])

        file_names_wo_ext = i_path.name[:-7]
        with h5py.File(f"./rvs_mean_spectrum/{file_names_wo_ext}.h5", "w") as f:
            f.create_dataset("source_id", data=file_path_f["source_id"].data)
            f.create_dataset("solution_id", data=file_path_f["solution_id"].data)
            f.create_dataset("ra", data=file_path_f["ra"].data)
            f.create_dataset("dec", data=file_path_f["dec"].data)
            f.create_dataset("flux", data=flux)
            f.create_dataset("flux_error", data=flux_error)


def compile_rvs_h5():
    for i_path in tqdm.tqdm(
        list(astro_data_path.joinpath("Gaia", "gdr3", "Spectroscopy", "rvs_mean_spectrum").glob(
            "*.csv.gz"
        )),
        desc="RVS spec",
    ):
        file_path_f = ascii.read(i_path)
        flux = np.vstack(file_path_f["flux"])
        flux_error = np.vstack(file_path_f["flux_error"])

        file_names_wo_ext = i_path.name[:-7]
        with h5py.File(f"./rvs_mean_spectrum/{file_names_wo_ext}.h5", "w") as f:
            f.create_dataset("source_id", data=file_path_f["source_id"].data)
            f.create_dataset("solution_id", data=file_path_f["solution_id"].data)
            f.create_dataset("ra", data=file_path_f["ra"].data)
            f.create_dataset("dec", data=file_path_f["dec"].data)
            f.create_dataset("flux", data=flux)
            f.create_dataset("flux_error", data=flux_error)


def comile_xp_mean_spec_h5():
    for i_path in tqdm.tqdm(
        list(astro_data_path.joinpath(
            "Gaia", "gdr3", "Spectroscopy", "xp_sampled_mean_spectrum"
        ).glob("*.csv.gz")),
        desc="XP specs",
    ):
        file_path_f = ascii.read(i_path)
        flux = np.vstack(file_path_f["flux"])
        flux_error = np.vstack(file_path_f["flux_error"])

        file_names_wo_ext = i_path.name[:-7]
        with h5py.File(f"./xp_sampled_mean_spectrum/{file_names_wo_ext}.h5", "w") as f:
            f.create_dataset("source_id", data=file_path_f["source_id"].data)
            f.create_dataset("solution_id", data=file_path_f["solution_id"].data)
            f.create_dataset("ra", data=file_path_f["ra"].data)
            f.create_dataset("dec", data=file_path_f["dec"].data)
            f.create_dataset("flux", data=flux)
            f.create_dataset("flux_error", data=flux_error)


def compile_gaia_sql_db(do_gaia_source_table=True, do_gaia_astrophysical_table=True, indexing=True):
    """
    This function compile Gaia SQL database
    """
    # The whole script takes about ~24 hours to complete
    Path(gaia_sql_db_path).touch()
    conn = sqlite3.connect(gaia_sql_db_path)
    c = conn.cursor()

    # we have added "grvs_mag" to the table on top of gaia_source_lite on Gaia Archive
    # will take ~11 hours to run
    if do_gaia_source_table:
        # =================== setup Gaia schema and first two tables ===================
        # this section will take ~30 minutes to run
        for schema in [
            "gaia_source_lite_schema",
            "allwise_best_neighbour_schema",
            "tmasspscxsc_best_neighbour_schema",
        ]:
            schema_filename = os.path.join(
                os.path.dirname(__file__), "sql_schema", f"{schema}"
            )
            with open(schema_filename) as f:
                lines = f.read().replace("\n", "")
            c.execute(lines)

        for name, table_name in zip(
            [_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT, _GAIA_DR3_2MASS_NEIGHBOUR_PARENT],
            ["allwise_best_neighbour", "tmasspscxsc_best_neighbour"],
        ):
            for p in tqdm.tqdm(list(name.glob("*.csv.gz"))):
                # load the data into a Pandas DataFrame
                data = pd.read_csv(p, header=0, sep=",")
                # write the data to a sqlite table
                data.to_sql(f"{table_name}", conn, if_exists="append", index=False)

        # =================== populate gaia_source lite table ===================
        # use "Int32" type for int columns with NaN
        for p in tqdm.tqdm(list(_GAIA_DR3_GAIASOURCE_PARENT.glob("*.csv.gz"))):
            dtypes = {
                "source_id": np.int64,
                "random_index": np.int64,
                "ra": np.float64,
                "ra_error": np.float64,
                "dec": np.float64,
                "dec_error": np.float64,
                "parallax": np.float64,
                "parallax_error": np.float64,
                "parallax_over_error": np.float32,
                "pmra": np.float64,
                "pmra_error": np.float64,
                "pmdec": np.float64,
                "pmdec_error": np.float64,
                "ra_dec_corr": np.float32,
                "ra_parallax_corr": np.float32,
                "ra_pmra_corr": np.float32,
                "ra_pmdec_corr": np.float32,
                "dec_parallax_corr": np.float32,
                "dec_pmra_corr": np.float32,
                "dec_pmdec_corr": np.float32,
                "parallax_pmra_corr": np.float32,
                "parallax_pmdec_corr": np.float32,
                "pmra_pmdec_corr": np.float32,
                "astrometric_params_solved": "Int32",
                "nu_eff_used_in_astrometry": np.float32,
                "pseudocolour": np.float64,
                "pseudocolour_error": np.float64,
                "astrometric_matched_transits": "Int32",
                "ipd_gof_harmonic_amplitude": np.float32,
                "ipd_frac_multi_peak": "Int32",
                "ipd_frac_odd_win": "Int32",
                "ruwe": np.float32,
                "phot_g_mean_flux": np.float64,
                "phot_g_mean_flux_over_error": np.float32,
                "phot_g_mean_mag": np.float32,
                "phot_bp_mean_flux": np.float64,
                "phot_bp_mean_flux_over_error": np.float32,
                "phot_bp_mean_mag": np.float32,
                "phot_rp_mean_flux": np.float64,
                "phot_rp_mean_flux_over_error": np.float32,
                "phot_rp_mean_mag": np.float32,
                "phot_bp_rp_excess_factor": np.float32,
                "bp_rp": np.float32,
                "radial_velocity": np.float64,
                "radial_velocity_error": np.float64,
                "rv_nb_transits": "Int32",
                "rv_expected_sig_to_noise": np.float32,
                "rv_renormalised_gof": np.float32,
                "rv_chisq_pvalue": np.float32,
                "rvs_spec_sig_to_noise": np.float32,
                "grvs_mag": np.float32,
                "l": np.float64,
                "b": np.float64,
                "has_xp_continuous": bool,
                "has_xp_sampled": bool,
                "has_rvs": bool,
            }
            data = pd.read_csv(
                p, header=1, sep=",", skiprows=999, usecols=dtypes.keys(), dtype=dtypes
            )
            # write the data to a sqlite table
            data.to_sql("gaia_source", conn, if_exists="append", index=False)

    if do_gaia_astrophysical_table:
        schema_filename = os.path.join(os.path.dirname(__file__), "sql_schema", "astrophysical_parameters_lite_schema")

        with open(schema_filename) as f:
            lines = f.read().replace("\n", "")
        c.execute(lines)
        # =================== populate gaia_source lite table ===================
        # we have added "grvs_mag" to the table on top of gaia_source_lite on Gaia Archive
        # will take ~11 hours to run

        for p in tqdm.tqdm(list(_GAIA_DR3_ASTROPHYS_PARENT.glob("*.csv.gz"))):
            dtypes = {
                "source_id": np.int64,
                "classprob_dsc_combmod_quasar": np.float32,
                "classprob_dsc_combmod_galaxy": np.float32,
                "classprob_dsc_combmod_star": np.float32,
                "classprob_dsc_combmod_whitedwarf": np.float32,
                "classprob_dsc_combmod_binarystar": np.float32,
                "classprob_dsc_specmod_quasar": np.float32, 
                "classprob_dsc_specmod_galaxy": np.float32,
                "classprob_dsc_specmod_star": np.float32,
                "classprob_dsc_specmod_whitedwarf": np.float32,
                "classprob_dsc_specmod_binarystar": np.float32,
                "classprob_dsc_allosmod_quasar": np.float32,
                "classprob_dsc_allosmod_galaxy": np.float32,
                "classprob_dsc_allosmod_star": np.float32,
                "teff_gspphot": np.float32,
                "teff_gspphot_lower": np.float32,
                "teff_gspphot_upper": np.float32,
                "logg_gspphot": np.float32,
                "logg_gspphot_lower": np.float32,
                "logg_gspphot_upper": np.float32,
                "mh_gspphot": np.float32,
                "mh_gspphot_lower": np.float32,
                "mh_gspphot_upper": np.float32,
                "distance_gspphot": np.float32,
                "distance_gspphot_lower": np.float32,
                "distance_gspphot_upper": np.float32,
                "azero_gspphot": np.float32,
                "azero_gspphot_lower": np.float32,
                "azero_gspphot_upper": np.float32,
                "ag_gspphot": np.float32,
                "ag_gspphot_lower": np.float32,
                "ag_gspphot_upper": np.float32,
                "abp_gspphot": np.float32,
                "abp_gspphot_lower": np.float32,
                "abp_gspphot_upper": np.float32,
                "arp_gspphot": np.float32,
                "arp_gspphot_lower": np.float32,
                "arp_gspphot_upper": np.float32,
                "ebpminrp_gspphot": np.float32,
                "ebpminrp_gspphot_lower": np.float32,
                "ebpminrp_gspphot_upper": np.float32,
                "mg_gspphot": np.float32,
                "mg_gspphot_lower": np.float32,
                "mg_gspphot_upper": np.float32,
                "radius_gspphot": np.float32,
                "radius_gspphot_lower": np.float32,
                "radius_gspphot_upper": np.float32,
                "logposterior_gspphot": np.float32,
                "mcmcaccept_gspphot": np.float32,
                "libname_gspphot": str,
                "teff_gspspec": np.float32,
                "teff_gspspec_lower": np.float32,
                "teff_gspspec_upper": np.float32,
                "logg_gspspec": np.float32,
                "logg_gspspec_lower": np.float32,
                "logg_gspspec_upper": np.float32,
                "mh_gspspec": np.float32,
                "mh_gspspec_lower": np.float32,
                "mh_gspspec_upper": np.float32,
                "alphafe_gspspec": np.float32,
                "alphafe_gspspec_lower": np.float32,
                "alphafe_gspspec_upper": np.float32,
                "flags_gspspec": str,
                "activityindex_espcs": np.float32,
                "activityindex_espcs_uncertainty": np.float32,
                "activityindex_espcs_input": str
            }
            data = pd.read_csv(
                p, header=1, sep=",", skiprows=1540, usecols=dtypes.keys(), dtype=dtypes
            )
            # write the data to a sqlite table
            data.to_sql("astrophysical_parameters", conn, if_exists="append", index=False)

    # =================== indexing ===================
    if indexing:
        print("=================== indexing ===================")
        print("Start doing allwise_best_neighbour_sourceid_designation indexing")
        c.execute(
            """CREATE INDEX allwise_best_neighbour_sourceid_designation ON allwise_best_neighbour (source_id, original_ext_source_id);"""
        )
        print("Start doing tmasspscxsc_best_neighbour_sourceid_designation indexing")
        c.execute(
            """CREATE INDEX tmasspscxsc_best_neighbour_sourceid_designation ON tmasspscxsc_best_neighbour (source_id, original_ext_source_id);"""
        )


def compile_tmass_sql_db(indexing=True):
    """
    This function compile 2MASS point source SQL database
    """
    Path(tmass_sql_db_path).touch()
    conn = sqlite3.connect(tmass_sql_db_path)
    c = conn.cursor()

    # =================== 2MASS ===================
    # this section will take 1 hour to run
    schema_filename = os.path.join(os.path.dirname(__file__), "sql_schema", "twomass_psc_lite_schema")

    with open(schema_filename) as f:
        lines = f.read().replace("\n", "")
    c.execute(lines)

    # only the first part, not all actually
    # https://irsa.ipac.caltech.edu/data/2MASS/docs/releases/allsky/doc/sec2_2a.html
    tmass_allcol = [
        "ra",
        "dec",
        "err_maj",
        "err_min",
        "err_ang",
        "designation",
        # Primary Photometric Information
        "j_m",
        "j_cmsig",
        "j_msigcom",
        "j_snr",
        "h_m",
        "h_cmsig",
        "h_msigcom",
        "h_snr",
        "k_m",
        "k_cmsig",
        "k_msigcom",
        "k_snr",
        # Primary Source Quality Information
        "ph_qual",
        "rd_flg",
        "bl_flg",
        "cc_flg",
        "ndet",
        "prox",
        "pxpa",
        "pxcntr",
        "gal_contam",
        "mp_flg",
        # Additional Positional and Identification Information
        "pts_key/cntr",
        "hemis",
        "date",
        "scan",
        "glon",
        "glat",
        "x_scan",
        "jdate",
        # Additional Photometric Information
        "j_psfchi",
        "h_psfchi",
        "k_psfchi"
        "j_m_stdap",
        "j_msig_stdap",
        "h_m_stdap",
        "h_msig_stdap",
        "k_m_stdap",
        "k_msig_stdap",
        # Additional Source Quality Information
        "dist_edge_ns",
        "dist_edge_ew",
        "dist_edge_flg",
        "dup_src",
        "use_src",
        # Optical Source Association Information
        "a",
        "dist_opt",
        "phi_opt",
        "b_m_opt",
        "vr_m_opt",
        "nopt_mchs",
        # Cross-Index Information
        "ext_key",
        "scan_key",
        "coadd_key",
        "coadd"
    ]

    for p in tqdm.tqdm(list(_2MASS_PARENT.glob("psc_*.gz"))):
        dtypes = {
            "ra": np.float64,
            "dec": np.float64,
            "designation": str,
            "j_m": np.float32,
            "j_cmsig": np.float32,
            "j_msigcom": np.float32,
            "j_snr": np.float32,
            "h_m": np.float32,
            "h_cmsig": np.float32,
            "h_msigcom": np.float32,
            "h_snr": np.float32,
            "k_m": np.float32,
            "k_cmsig": np.float32,
            "k_msigcom": np.float32,
            "k_snr": np.float32,
            "ph_qual": str,
            "rd_flg": str,
            "bl_flg": str,
            "cc_flg": str,
            "ndet": str,
            "prox": np.float32,
        }
        data = pd.read_csv(
            p,
            header=None,
            sep="|",
            usecols=[tmass_allcol.index(i) for i in dtypes.keys()],
            names=dtypes.keys(),
            # turn null to proper NaN
            na_values=["\\N"],
            # dont allow white space in names since gaia best neightbour do not have white space
            converters={"designation": str.strip},
        )
        data = data.replace(r"\N", np.nan)
        data.astype(dtypes)

        # write the data to a sqlite table
        data.to_sql(f"twomass_psc", conn, if_exists="append", index=False)

    # =================== indexing ===================
    if indexing:
        # 9m46s
        print("Doing Indexing")
        c.execute(
            """CREATE INDEX twomass_psc_designation_mags ON twomass_psc (designation, j_m, h_m, k_m);"""
        )


def compile_allwise_sql_db(indexing=True):
    """
    This function compile allwise SQL database
    """
    Path(allwise_sql_db_path).touch()
    conn = sqlite3.connect(allwise_sql_db_path)
    c = conn.cursor()

    # this section will take ~16 hours to run
    schema_filename = os.path.join(os.path.dirname(__file__), "sql_schema", "allwise_lite_schema")

    with open(schema_filename) as f:
        lines = f.read().replace("\n", "")
    c.execute(lines)

    # only the first part, not all actually
    # https://wise2.ipac.caltech.edu/docs/release/allwise/expsup/sec2_1a.html
    allwise_allcol = [
        # Basic Position and Identification Information
        "designation",
        "ra",
        "dec",
        "sigra",
        "sigdec",
        "sigradec",
        "glon",
        "glat",
        "elon",
        "elat",
        "wx",
        "wy",
        "cntr",
        "source_id",
        "coadd_id",
        "src",
        # Primary Photometric Information
        "w1mpro",
        "w1sigmpro",
        "w1snr",
        "w1rchi2",
        "w2mpro",
        "w2sigmpro",
        "w2snr",
        "w2rchi2",
        "w3mpro",
        "w3sigmpro",
        "w3snr",
        "w3rchi2",
        "w4mpro",
        "w4sigmpro",
        "w4snr",
        "w4rchi2",
        "rchi2",
        "nb",
        "na",
        "w1sat",
        "w2sat",
        "w3sat",
        "w4sat",
        "satnum",
        # Motion Fit Parameters
        "ra_pm",
        "dec_pm",
        "sigra_pm",
        "sigdec_pm",
        "sigradec_pm",
        "pmra",
        "sigpmra",
        "pmdec",
        "sigpmdec",
        "w1rchi2_pm",
        "w2rchi2_pm",
        "w3rchi2_pm",
        "w4rchi2_pm",
        "rchi2_pm",
        "pmcode",
        # Measurement Quality and Source Reliability Information
        "cc_flags",
        "rel",
        "ext_flg",
        "var_flg",
        "ph_qual",
        "det_bit",
        "moon_lev",
        "w1nm",
        "w1m",
        "w2nm",
        "w2m",    
        "w3nm",
        "w3m",    
        "w4nm",
        "w4m",
        "w1cov",
        "w2cov",
        "w3cov",
        "w4cov",
        "w1cc_map",
        "w1cc_map_str",
        "w2cc_map",
        "w2cc_map_str",
        "w3cc_map",
        "w3cc_map_str",
        "w4cc_map",
        "w4cc_map_str",
        "use_src",
        "best_use_src", 
        "ngrp",
        # Additional Photometric Information
        "w1flux",
        "w1sigflux",
        "w1sky",
        "w1sigsk",
        "w1conf",
        "w2flux",
        "w2sigflux",
        "w2sky",
        "w2sigsk",
        "w2conf",
        "w3flux",
        "w3sigflux",
        "w3sky",
        "w3sigsk",
        "w3conf",
        "w4flux",
        "w4sigflux",
        "w4sky",
        "w4sigsk",
        "w4conf",
        "w1mag",
        "w1sigm",
        "w1flg",
        "w1mcor",
        "w2mag",
        "w2sigm",
        "w2flg",
        "w2mcor",
        "w3mag",
        "w3sigm",
        "w3flg",
        "w3mcor",
        "w4mag",
        "w4sigm",
        "w4flg",
        "w4mcor",
        "w1mag_1",
        "w1sigm_1",
        "w1flg_1",
        "w2mag_1",
        "w2sigm_1",
        "w2flg_1",
        "w3mag_1",
        "w3sigm_1",
        "w3flg_1",
        "w4mag_1",
        "w4sigm_1",
        "w4flg_1",
        "w1mag_2",
        "w1sigm_2",
        "w1flg_2",
        "w2mag_2",
        "w2sigm_2",
        "w2flg_2",
        "w3mag_2",
        "w3sigm_2",
        "w3flg_2",
        "w4mag_2",
        "w4sigm_2",
        "w4flg_2",
        "w1mag_3",
        "w1sigm_3",
        "w1flg_3",
        "w2mag_3",
        "w2sigm_3",
        "w2flg_3",
        "w3mag_3",
        "w3sigm_3",
        "w3flg_3",
        "w4mag_3",
        "w4sigm_3",
        "w4flg_3",
        "w1mag_4",
        "w1sigm_4",
        "w1flg_4",
        "w2mag_4",
        "w2sigm_4",
        "w2flg_4",
        "w3mag_4",
        "w3sigm_4",
        "w3flg_4",
        "w4mag_4",
        "w4sigm_4",
        "w4flg_4",
        "w1mag_5",
        "w1sigm_5",
        "w1flg_5",
        "w2mag_5",
        "w2sigm_5",
        "w2flg_5",
        "w3mag_5",
        "w3sigm_5",
        "w3flg_5",
        "w4mag_5",
        "w4sigm_5",
        "w4flg_5",
        "w1mag_6",
        "w1sigm_6",
        "w1flg_6",
        "w2mag_6",
        "w2sigm_6",
        "w2flg_6",
        "w3mag_6",
        "w3sigm_6",
        "w3flg_6",
        "w4mag_6",
        "w4sigm_6",
        "w4flg_6",
        "w1mag_7",
        "w1sigm_7",
        "w1flg_7",
        "w2mag_7",
        "w2sigm_7",
        "w2flg_7",
        "w3mag_7",
        "w3sigm_7",
        "w3flg_7",
        "w4mag_7",
        "w4sigm_7",
        "w4flg_7",
        "w1mag_",
        "w1sigm_8",
        "w1flg_8",
        "w2mag_8",
        "w2sigm_8",
        "w2flg_8",
        "w3mag_8",
        "w3sigm_8",
        "w3flg_8",
        "w4mag_8",
        "w4sigm_8",
        "w4flg_8",
        "w1magp",
        "w1sigp1",
        "w1sigp2",
        "w1k",
        "w1ndf",
        "w1mlq",
        "w1mjdmin",
        "w1mjdmax",
        "w1mjdmean",
        "w2magp",
        "w2sigp1",
        "w2sigp2",
        "w2k",
        "w2ndf",
        "w2mlq",
        "w2mjdmin",
        "w2mjdmax",
        "w2mjdmean",
        "w3magp",
        "w3sigp1",
        "w3sigp2",
        "w3k",
        "w3ndf",
        "w3mlq",
        "w3mjdmin",
        "w3mjdmax",
        "w3mjdmean",
        "w4magp",
        "w4sigp1",
        "w4sigp2",
        "w4k",
        "w4ndf",
        "w4mlq",
        "w4mjdmin",
        "w4mjdmax",
        "w4mjdmean",
        "rho12",
        "rho23",
        "rho34",
        "q12",
        "q23",
        "q34",
        "xscprox",
        "w1rsemi",
        "w1ba",
        "w1pa",
        "w1gmag",
        "w1gerr",
        "w1gflg",
        "w2rsemi",
        "w2ba",
        "w2pa",
        "w2gmag",
        "w2gerr",
        "w2gflg",
        "w3rsemi",
        "w3ba",
        "w3pa",
        "w3gmag",
        "w3gerr",
        "w3gflg",
        "w4rsemi",
        "w4ba",
        "w4pa",
        "w4gmag",
        "w4gerr",
        "w4gflg",
        "tmass_key",
        "r_2mass",
        "pa_2mass",
        "n_2mass",
        "j_m_2mass",
        "j_msig_2mass",
        "h_m_2mass",
        "h_msig_2mass",
        "k_m_2mass",
        "k_msig_2mass",
        "x",
        "y",
        "z",
        "spt_ind",
        "htm20",
    ]

    for p in tqdm.tqdm(list(_ALLWISE_PARENT.glob("wise-allwise-cat-*.bz2"))):
        dtypes = {
            "designation": str,  # designation does not seems to be unique, dont use it as primary key
            "ra": np.float64,
            "dec": np.float64,
            "sigra": np.float32,
            "sigdec": np.float32,
            "sigradec": np.float32,
            "w1mpro": np.float32,
            "w1sigmpro": np.float32,
            "w1snr": np.float32,
            "w2mpro": np.float32,
            "w2sigmpro": np.float32,
            "w2snr": np.float32,
            "w3mpro": np.float32,
            "w3sigmpro": np.float32,
            "w3snr": np.float32,
            "w4mpro": np.float32,
            "w4sigmpro": np.float32,
            "w4snr": np.float32,
            "nb": "Int32",
            "na": "Int32",
            "cc_flags": str,
            "ext_flg": "Int32",
            "var_flg": str,
            "ph_qual": str,
            "w1mjdmean": np.float64,
            "w2mjdmean": np.float64,
            "w3mjdmean": np.float64,
            "w4mjdmean": np.float64,
            "w1gmag": np.float32,
            "w1gerr": np.float32,
            "w2gmag": np.float32,
            "w2gerr": np.float32,
            "w3gmag": np.float32,
            "w3gerr": np.float32,
            "w4gmag": np.float32,
            "w4gerr": np.float32,
        }
        data = pd.read_csv(
            p,
            header=None,
            sep="|",
            usecols=[allwise_allcol.index(i) for i in dtypes.keys()],
            names=dtypes.keys(),
        )
        data.astype(dtypes)
        
        # write the data to a sqlite table
        data.to_sql(f"allwise", conn, if_exists="append", index=False)

    # =================== indexing ===================
    if indexing:
        # 22m56s
        print("Doing Indexing")
        c.execute(
            """CREATE INDEX allwise_designation_mags ON allwise (designation, w1mpro, w2mpro, w3mpro, w4mpro, w1snr, w2snr, w3snr, w4snr, ph_qual);"""
        )
