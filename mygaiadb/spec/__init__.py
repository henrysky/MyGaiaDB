import h5py
import tqdm
import numpy as np
from .. import astro_data_path, gaia_xp_coeff_h5_path


def yield_xp_coeffs(source_ids, assume_unique=True, rdcc_nbytes=16*1024**3, rdcc_nslots=10e7):
    """
    Function to yield XP coeffs according to their healpixs from source_id

    Parameters
    ----------
    source_ids: (int, list, ndarray)
        source id
    assume_unique: bool
        whether to assume the list of source id is unique
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
        current_source_ids = source_ids[good_idx]
        spec_f = h5f[f"{reference_file['file'][i]}"]
        matches, idx1, idx2 = np.intersect1d(
            current_source_ids,
            spec_f["source_id"][()],
            assume_unique=assume_unique,
            return_indices=True,
        )
        bp_coeffs = spec_f["bp_coefficients"][()]
        rp_coeffs = spec_f["rp_coefficients"][()]

        if len(matches) > 0:
            coeffs = np.hstack([bp_coeffs[idx2], rp_coeffs[idx2]])
            yield coeffs, np.arange(total_num)[good_idx][idx1]
        else:
            # TODO: I think maybe should not pass, need more test
            pass
