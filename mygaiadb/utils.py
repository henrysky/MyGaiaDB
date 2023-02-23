import numpy as np
import astropy.units as u


def SameSide(A, R):
    """Place the angle into same quadrant as referant"""
    A_rad = A / 180 * np.pi
    R_rad = R / 180 * np.pi
    return np.where(
        (
            ((np.sign(np.cos(A_rad))) == (np.sign(np.cos(R_rad)))) == ((np.sign(np.sin(A_rad))) == (np.sign(np.sin(R_rad))))
        ),
        (A + 36000) % 360,
        (A + 36180) % 360,
    )


def radec_to_ecl(ra, dec):
    """
    refers to section 1.5.3 in https://www.cosmos.esa.int/documents/532822/552851/vol1_all.pdf

    this relation is good for Gaia with epoch 2016, accurate to almost machine precision
    """
    e = 23.43928083333333 / 180 * np.pi
    ra_rad = np.asarray(ra) / 180. * np.pi + (0.05542 * u.arcsec).to(u.rad).value
    dec_rad = np.asarray(dec) / 180. * np.pi
    l = np.arctan(
        np.tan(ra_rad) * np.cos(e) + np.tan(dec_rad) * np.sin(e) / np.cos(ra_rad)
    ) / (np.pi / 180.)
    b = np.arcsin(
        np.sin(dec_rad) * np.cos(e) - np.cos(dec_rad) * np.sin(e) * np.sin(ra_rad)
    ) / (np.pi / 180.)
    l = SameSide(l, ra_rad)
    return l, b
