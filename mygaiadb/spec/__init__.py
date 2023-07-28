import h5py
import tqdm
import numpy as np
from .. import astro_data_path, gaia_xp_coeff_h5_path


def yield_xp_coeffs(source_ids, assume_unique=True, return_errors=False, return_additional_columns=None, rdcc_nbytes=16*1024**3, rdcc_nslots=10e7):
    """
    Function to yield XP coeffs according to their healpixs from source_id

    Parameters
    ----------
    source_ids: Union[int, list, ndarray]
        Gaia source id
    assume_unique: bool
        Whether to assume the list of Gaia source id is unique
    return_errors: bool
        Whether to return xp coeffs error
    return_additional_columns: Union[None, list] 
        Additional columns to return
        If you want coefficients error, please use return_errors=True
    rdcc_nbytes: int
        h5py cache in bytes
    rdcc_nslots: int
        h5py cache number of slots
    """
    reduced_source_ids = source_ids // 8796093022208
    total_num = len(source_ids)

    h5f = h5py.File(gaia_xp_coeff_h5_path, "r", rdcc_nbytes=rdcc_nbytes, rdcc_nslots=rdcc_nslots)
    file_names = list(h5f.keys())

    # Extract HEALPix level-8 from file name
    healpix_8_min = [
        int(file[0:file.rfind("-")]) for file in file_names
    ]
    healpix_8_max = [
        int(file[file.rfind("-") + 1:]) for file in file_names
    ]
    reference_file = {
        "file": file_names,
        "healpix8_min": np.asarray(healpix_8_min),
        "healpix8_max": np.asarray(healpix_8_max),
    }
    for i in tqdm.tqdm(range(len(reference_file["healpix8_min"]))):
        good_idx = (reference_file["healpix8_min"][i] <= reduced_source_ids) & (reduced_source_ids <= reference_file["healpix8_max"][i])
        if np.sum(good_idx) > 0:
            current_source_ids = source_ids[good_idx]
            spec_f = h5f[f"{reference_file['file'][i]}"]
            matches, idx1, idx2 = np.intersect1d(
                current_source_ids,
                spec_f["source_id"][()],
                assume_unique=assume_unique,
                return_indices=True,
            )
            # load all to memory to be faster, although this is kinda memory intensive
            bp_coeffs = spec_f["bp_coefficients"][()]
            rp_coeffs = spec_f["rp_coefficients"][()]

            if len(matches) > 0:
                coeffs = np.hstack([bp_coeffs[idx2], rp_coeffs[idx2]])
                if not return_errors:
                    if return_additional_columns is None:
                        yield coeffs, np.arange(total_num)[good_idx][idx1]
                    else:
                        extra_columns = tuple(spec_f[i][()][idx2] for i in return_additional_columns)
                        yield coeffs, np.arange(total_num)[good_idx][idx1], *extra_columns
                else:
                    bp_coeffs_err = spec_f["bp_coefficient_errors"][()]
                    rp_coeffs_err = spec_f["rp_coefficient_errors"][()]
                    coeffs_err = np.hstack([bp_coeffs_err[idx2], rp_coeffs_err[idx2]])
                    if return_additional_columns is None:
                        yield coeffs, np.arange(total_num)[good_idx][idx1], coeffs_err
                    else:
                        extra_columns = tuple(spec_f[i][()][idx2] for i in return_additional_columns)
                        yield coeffs, np.arange(total_num)[good_idx][idx1], coeffs_err, *extra_columns
            else:  # this is the case where the source_id is within healpix but does not have xp coeffs
                pass
