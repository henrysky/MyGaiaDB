import os
import glob
import h5py
import tqdm
from mygaiadb import astro_data_path, gaia_xp_coeff_h5_path

# =================== converting xp coeffs to a single h5 ===================

base_path = os.path.join(
    astro_data_path,
    "gaia_mirror",
    "Gaia",
    "gdr3",
    "Spectroscopy",
    "xp_continuous_mean_spectrum",
)
file_paths = glob.glob(os.path.join(base_path, "*.h5"))

if len(file_paths) == 0:
    raise FileNotFoundError(f"Gaia data does not exist at {base_path}")

file_names = [os.path.basename(x) for x in file_paths]
healpix_8_min = [int(file[file.find("_") + 1 : file.rfind("-")]) for file in file_names]
healpix_8_max = [
    int(file[file.rfind("-") + 1 : file.rfind(".h5")]) for file in file_names
]

h5f = h5py.File(gaia_xp_coeff_h5_path, "w")
for i in tqdm.tqdm(range(len(file_paths))):
    temp_h5_data = h5py.File(file_paths[i], "r")
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
    gp.create_dataset("bp_n_measurements", data=temp_h5_data["bp_n_measurements"][()])
    gp.create_dataset(
        "bp_n_rejected_measurements", data=temp_h5_data["bp_n_rejected_measurements"][()]
    )
    gp.create_dataset(
        "bp_standard_deviation", data=temp_h5_data["bp_standard_deviation"][()]
    )
    gp.create_dataset("bp_chi_squared", data=temp_h5_data["bp_chi_squared"][()])
    gp.create_dataset(
        "bp_coefficient_errors", data=temp_h5_data["bp_coefficient_errors"][()]
    )
    gp.create_dataset("bp_coefficients", data=temp_h5_data["bp_coefficients"][()])
    gp.create_dataset("bp_n_relevant_bases", data=temp_h5_data["bp_n_relevant_bases"][()])
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
    gp.create_dataset("rp_n_measurements", data=temp_h5_data["rp_n_measurements"][()])
    gp.create_dataset(
        "rp_n_rejected_measurements", data=temp_h5_data["rp_n_rejected_measurements"][()]
    )
    gp.create_dataset(
        "rp_standard_deviation", data=temp_h5_data["rp_standard_deviation"][()]
    )
    gp.create_dataset("rp_chi_squared", data=temp_h5_data["rp_chi_squared"][()])
    gp.create_dataset(
        "rp_coefficient_errors", data=temp_h5_data["rp_coefficient_errors"][()]
    )
    gp.create_dataset("rp_coefficients", data=temp_h5_data["rp_coefficients"][()])
    gp.create_dataset("rp_n_relevant_bases", data=temp_h5_data["rp_n_relevant_bases"][()])
    gp.create_dataset(
        "rp_relative_shrinking", data=temp_h5_data["rp_relative_shrinking"][()]
    )
    temp_h5_data.close()
h5f.close()
