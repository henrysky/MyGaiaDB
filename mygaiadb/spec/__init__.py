import h5py
import tqdm
import numpy as np
from .. import gaia_xp_coeff_h5_path


def yield_xp_coeffs(
    source_ids,
    assume_unique=True,
    return_errors=False,
    return_additional_columns=None,
    rdcc_nbytes=16 * 1024**3,
    rdcc_nslots=10e7,
):
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
    source_ids = np.asarray(source_ids, dtype=np.int64)
    reduced_source_ids = source_ids // 8796093022208
    bad_source_ids = source_ids < 1
    total_num = len(source_ids)

    h5f = h5py.File(
        gaia_xp_coeff_h5_path, "r", rdcc_nbytes=rdcc_nbytes, rdcc_nslots=rdcc_nslots
    )
    file_names = list(h5f.keys())

    # Extract HEALPix level-8 from file name
    healpix_8_min = [int(file[0 : file.rfind("-")]) for file in file_names]
    healpix_8_max = [int(file[file.rfind("-") + 1 :]) for file in file_names]
    reference_file = {
        "file": file_names,
        "healpix8_min": np.asarray(healpix_8_min),
        "healpix8_max": np.asarray(healpix_8_max),
    }
    # for i in tqdm.tqdm(range(len(reference_file["healpix8_min"]))):
    for min, max, f in zip(
        tqdm.tqdm(reference_file["healpix8_min"]),
        reference_file["healpix8_max"],
        reference_file["file"],
    ):
        good_idx = (
            (min <= reduced_source_ids) & (reduced_source_ids <= max) & ~bad_source_ids
        )
        if np.sum(good_idx) > 0:
            current_source_ids = source_ids[good_idx]
            spec_f = h5f[f]
            matches, idx1, idx2 = np.intersect1d(
                current_source_ids,
                np.asarray(spec_f["source_id"][()], dtype=np.int64),
                assume_unique=assume_unique,
                return_indices=True,
            )

            if (size := len(matches)) > 0:
                # warp negative indices to positive indices
                idx2 = idx2 % spec_f["source_id"].len()

                # dealing with sorting since h5py does not support fancy indexing
                idx_argsort = np.argsort(idx2)
                idx2_sorted = idx2[idx_argsort]
                idx2_inv_argsort = np.argsort(idx_argsort)

                coeffs = np.zeros((size, 110), dtype=spec_f["bp_coefficients"].dtype)
                try:
                    coeffs[:, :55] = (spec_f["bp_coefficients"][idx2_sorted])[
                        idx2_inv_argsort
                    ]
                except TypeError:
                    raise TypeError(
                        "Indexing elements must be in increasing order. Possible issues include your source_id array is not unique but you set 'assume_unique=True'"
                    )
                coeffs[:, 55:] = (spec_f["rp_coefficients"][idx2_sorted])[
                    idx2_inv_argsort
                ]
                if not return_errors:
                    if return_additional_columns is None:
                        yield coeffs, np.arange(total_num)[good_idx][idx1]
                    else:
                        extra_columns = tuple(
                            spec_f[i][()][idx2] for i in return_additional_columns
                        )
                        yield (
                            coeffs,
                            np.arange(total_num)[good_idx][idx1],
                            *extra_columns,
                        )
                else:
                    coeffs_err = np.zeros(
                        (size, 110), dtype=spec_f["bp_coefficient_errors"].dtype
                    )
                    coeffs_err[:, :55] = (spec_f["bp_coefficient_errors"][idx2_sorted])[
                        idx2_inv_argsort
                    ]
                    coeffs_err[:, 55:] = (spec_f["rp_coefficient_errors"][idx2_sorted])[
                        idx2_inv_argsort
                    ]
                    if return_additional_columns is None:
                        yield coeffs, np.arange(total_num)[good_idx][idx1], coeffs_err
                    else:
                        extra_columns = tuple(
                            spec_f[i][()][idx2] for i in return_additional_columns
                        )
                        yield (
                            coeffs,
                            np.arange(total_num)[good_idx][idx1],
                            coeffs_err,
                            *extra_columns,
                        )
            else:  # this is the case where the source_id is within healpix but does not have xp coeffs
                pass
