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
    _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT,
    _GAIA_DR3_2MASS_NEIGHBOUR_PARENT,
)
from .. import astro_data_path, gaia_sql_db_path, gaia_xp_coeff_h5_path


def compile_xp_continuous_allinone_h5():
    base_path = astro_data_path.joinpath(
        "gaia_mirror",
        "Gaia",
        "gdr3",
        "Spectroscopy",
        "xp_continuous_mean_spectrum",
    )
    file_paths = base_path.glob("*.h5")

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
        astro_data_path.joinpath(
            "Gaia/gdr3/Spectroscopy/xp_continuous_mean_spectrum/"
        ).glob("*.csv.gz"),
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
        astro_data_path.joinpath("Gaia/gdr3/Spectroscopy/rvs_mean_spectrum/").glob(
            "*.csv.gz"
        ),
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
        astro_data_path.joinpath("Gaia/gdr3/Spectroscopy/rvs_mean_spectrum/").glob(
            "*.csv.gz"
        ),
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
        astro_data_path.joinpath(
            "Gaia/gdr3/Spectroscopy/xp_sampled_mean_spectrum/"
        ).glob("*.csv.gz"),
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


def compile_gaia_sql_db(do_gaia_source_table=True, do_indexing=True):
    # The whole script takes about ~24 hours to complete
    db_filename = gaia_sql_db_path
    Path(db_filename).touch()
    conn = sqlite3.connect(db_filename)
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
            glob_paths = name.glob("*.csv.gz")
            for p in tqdm.tqdm(glob_paths):
                # load the data into a Pandas DataFrame
                data = pd.read_csv(p, header=0, sep=",")
                # write the data to a sqlite table
                data.to_sql(f"{table_name}", conn, if_exists="append", index=False)

        # =================== populate gaia_source lite table ===================

        glob_paths = _GAIA_DR3_GAIASOURCE_PARENT.glob("*.csv.gz")
        # use "Int32" type for int columns with NaN
        for p in tqdm.tqdm(glob_paths):
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

    # =================== indexing ===================
    if do_indexing:
        print("=================== indexing ===================")
        print("Start doing allwise_best_neighbour_sourceid_designation indexing")
        c.execute(
            """CREATE INDEX allwise_best_neighbour_sourceid_designation ON allwise_best_neighbour (source_id, original_ext_source_id);"""
        )
        print("Start doing tmasspscxsc_best_neighbour_sourceid_designation indexing")
        c.execute(
            """CREATE INDEX tmasspscxsc_best_neighbour_sourceid_designation ON tmasspscxsc_best_neighbour (source_id, original_ext_source_id);"""
        )
