import numpy as np
from numpy.typing import ArrayLike, NDArray


def radec_to_ecl(ra: ArrayLike, dec: ArrayLike) -> tuple[NDArray, NDArray]:
    """
    refers to section 1.5.3 in https://www.cosmos.esa.int/documents/532822/552851/vol1_all.pdf

    this relation is good for Gaia with epoch 2016, accurate to almost machine precision

    Parameters
    ----------
    ra : float or array
        Right ascension in degrees
    dec : float or array
        Declination in degrees

    Returns
    -------
    ecl_lon : array
        Ecliptic longitude in degrees
    ecl_lat : array
        Ecliptic latitude in degrees
    """
    # Obliquity of the ecliptic at J2016.0 is ~ 23.43928083333333 degrees
    epsilon_rad = 0.4090926248412669  # rad

    # the ICRS origin is shifted in the equatorial plane from Γ by 0.05542 arcsec
    ra_ecl_icrsshif = 2.686837420709048e-07  # rad

    # Convert RA and DEC from degrees to radians
    ra_rad = np.deg2rad(np.asarray(ra)) + ra_ecl_icrsshif
    dec_rad = np.deg2rad(np.asarray(dec))

    # Calculate ecliptic longitude (lambda)
    sin_lambda = np.sin(ra_rad) * np.cos(epsilon_rad) + np.tan(dec_rad) * np.sin(
        epsilon_rad
    )
    cos_lambda = np.cos(ra_rad)
    ecl_lon = np.arctan2(sin_lambda, cos_lambda)

    # Calculate ecliptic latitude (beta)
    sin_beta = np.sin(dec_rad) * np.cos(epsilon_rad) - np.cos(dec_rad) * np.sin(
        epsilon_rad
    ) * np.sin(ra_rad)
    ecl_lat = np.arcsin(sin_beta)

    # Convert lambda and beta to degrees
    ecl_lon = np.rad2deg(ecl_lon) % 360  # [0, 360) degrees
    ecl_lat = np.rad2deg(ecl_lat)

    return ecl_lon, ecl_lat
