import os
import h5py
import glob
import tqdm
import numpy as np
from astropy.io import ascii
from mygaiadb import astro_data_path


# =================== converting xp coeffs to h5 ===================

# whether to save correlation matrix too (warning: those are big in size)
save_correlation_matrix = False

for file_path in tqdm.tqdm(glob.glob(os.path.joing(astro_data_path, "Gaia/gdr3/Spectroscopy/xp_continuous_mean_spectrum/*.csv.gz"))):
    gaia_f = ascii.read(file_path)

    bp_basis_function_id = np.asarray(gaia_f["bp_basis_function_id"], dtype=np.int16)
    bp_degrees_of_freedom = np.asarray(gaia_f["bp_degrees_of_freedom"], dtype=np.int16)
    bp_n_parameters = np.asarray(gaia_f["bp_n_parameters"], dtype=np.int8)
    bp_n_measurements = np.asarray(gaia_f["bp_n_measurements"], dtype=np.int16)
    bp_n_rejected_measurements = np.asarray(gaia_f["bp_n_rejected_measurements"], dtype=np.int16)
    bp_standard_deviation = np.asarray(gaia_f["bp_standard_deviation"], dtype=np.float32)
    bp_chi_squared = np.asarray(gaia_f["bp_chi_squared"], dtype=np.float32)
    if save_correlation_matrix:
        bp_coefficient_correlations = np.stack(gaia_f["bp_coefficient_correlations"])
    bp_coefficient_errors = np.stack(gaia_f["bp_coefficient_errors"])
    bp_coefficients = np.stack(gaia_f["bp_coefficients"])
    bp_n_relevant_bases = np.asarray(gaia_f["bp_n_relevant_bases"], dtype=np.int16)
    bp_relative_shrinking = np.asarray(gaia_f["bp_relative_shrinking"], dtype=np.float32)

    rp_basis_function_id = np.asarray(gaia_f["rp_basis_function_id"], dtype=np.int16)
    rp_degrees_of_freedom = np.asarray(gaia_f["rp_degrees_of_freedom"], dtype=np.int16)
    rp_n_parameters = np.asarray(gaia_f["rp_n_parameters"], dtype=np.int8)
    rp_n_measurements = np.asarray(gaia_f["rp_n_measurements"], dtype=np.int16)
    rp_n_rejected_measurements = np.asarray(gaia_f["rp_n_rejected_measurements"], dtype=np.int16)
    rp_standard_deviation = np.asarray(gaia_f["rp_standard_deviation"], dtype=np.float32)
    rp_chi_squared = np.asarray(gaia_f["rp_chi_squared"], dtype=np.float32)
    if save_correlation_matrix:
        rp_coefficient_correlations = np.stack(gaia_f["rp_coefficient_correlations"])
    rp_coefficient_errors = np.stack(gaia_f["rp_coefficient_errors"])
    rp_coefficients = np.stack(gaia_f["rp_coefficients"])
    rp_n_relevant_bases = np.asarray(gaia_f["rp_n_relevant_bases"], dtype=np.int16)
    rp_relative_shrinking = np.asarray(gaia_f["rp_relative_shrinking"], dtype=np.float32)


    file_names_wo_ext = os.path.basename(file_path)[:-7]
    with h5py.File(f"./xp_continuous_mean_spectrum/{file_names_wo_ext}.h5","w") as h5f:
        h5f.create_dataset("source_id",
                            data=gaia_f["source_id"].data)
        h5f.create_dataset("solution_id",
                            data=gaia_f["solution_id"].data)
        h5f.create_dataset("bp_basis_function_id ", data=bp_basis_function_id)
        h5f.create_dataset("bp_degrees_of_freedom ", data=bp_degrees_of_freedom)
        h5f.create_dataset("bp_n_parameters ", data=bp_n_parameters)
        h5f.create_dataset("bp_n_measurements ", data=bp_n_measurements)
        h5f.create_dataset("bp_n_rejected_measurements ", data=bp_n_rejected_measurements)
        h5f.create_dataset("bp_standard_deviation ", data=bp_standard_deviation)
        h5f.create_dataset("bp_chi_squared ", data=bp_chi_squared)
        if save_correlation_matrix:
            h5f.create_dataset("bp_coefficient_correlations ", data=bp_coefficient_correlations)
        h5f.create_dataset("bp_coefficient_errors", 
                            data=bp_coefficient_errors)
        h5f.create_dataset("bp_coefficients", 
                            data=bp_coefficients)
        h5f.create_dataset("bp_n_relevant_bases", data=bp_n_relevant_bases)
        h5f.create_dataset("bp_relative_shrinking", data=bp_relative_shrinking)
        
        h5f.create_dataset("rp_basis_function_id ", data=rp_basis_function_id)
        h5f.create_dataset("rp_degrees_of_freedom ", data=rp_degrees_of_freedom)
        h5f.create_dataset("rp_n_parameters ", data=rp_n_parameters)
        h5f.create_dataset("rp_n_measurements ", data=rp_n_measurements)
        h5f.create_dataset("rp_n_rejected_measurements ", data=rp_n_rejected_measurements)
        h5f.create_dataset("rp_standard_deviation ", data=rp_standard_deviation)
        h5f.create_dataset("rp_chi_squared ", data=rp_chi_squared)
        if save_correlation_matrix:
            h5f.create_dataset("rp_coefficient_correlations ", data=rp_coefficient_correlations)
        h5f.create_dataset("rp_coefficient_errors", 
                            data=rp_coefficient_errors)
        h5f.create_dataset("rp_coefficients", 
                            data=rp_coefficients)
        h5f.create_dataset("rp_n_relevant_bases", data=rp_n_relevant_bases)
        h5f.create_dataset("rp_relative_shrinking", data=rp_relative_shrinking)
        h5f.close()


# =================== converting rvs spec to h5 ===================

for i_path in tqdm.tqdm(glob.glob(os.path.joing(astro_data_path, "Gaia/gdr3/Spectroscopy/rvs_mean_spectrum/*.csv.gz"))):
    gaia_f = ascii.read(i_path)
    flux = np.vstack(gaia_f["flux"])
    flux_error = np.vstack(gaia_f["flux_error"])
    
    file_names_wo_ext = os.path.basename(i_path)[:-7]
    with h5py.File(f"./rvs_mean_spectrum/{file_names_wo_ext}.h5","w") as f:
        f.create_dataset("source_id",
                        data=gaia_f["source_id"].data)
        f.create_dataset("solution_id",
                        data=gaia_f["solution_id"].data)
        f.create_dataset("ra",
                        data=gaia_f["ra"].data)
        f.create_dataset("dec",
                        data=gaia_f["dec"].data)
        f.create_dataset("flux",
                        data=flux)
        f.create_dataset("flux_error",
                        data=flux_error)


# =================== converting xp spec spec to h5 ===================

for i_path in tqdm.tqdm(glob.glob(os.path.joing(astro_data_path, "Gaia/gdr3/Spectroscopy/xp_sampled_mean_spectrum/*.csv.gz"))):
    gaia_f = ascii.read(i_path)
    flux = np.vstack(gaia_f["flux"])
    flux_error = np.vstack(gaia_f["flux_error"])
    
    file_names_wo_ext = os.path.basename(i_path)[:-7]
    with h5py.File(f"./xp_sampled_mean_spectrum/{file_names_wo_ext}.h5","w") as f:
        f.create_dataset("source_id",
                        data=gaia_f["source_id"].data)
        f.create_dataset("solution_id",
                        data=gaia_f["solution_id"].data)
        f.create_dataset("ra",
                        data=gaia_f["ra"].data)
        f.create_dataset("dec",
                        data=gaia_f["dec"].data)
        f.create_dataset("flux",
                        data=flux)
        f.create_dataset("flux_error",
                        data=flux_error)
