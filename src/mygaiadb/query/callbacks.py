import importlib
import importlib.util
import inspect
import warnings
from abc import ABC, abstractmethod
from itertools import compress
from typing import Optional

import numpy as np

from mygaiadb.utils import radec_to_ecl


class QueryCallback(ABC):
    """
    Callback to add new column to SQL query on the fly

    Parameters
    ----------
    new_col_name : str
        Name of the new column you wan to add
    required_pkgs : list
        List of required packages to use this callback.
    """

    def __init__(self, new_col_name: str, required_pkgs: list[str]):
        self.new_col_name = new_col_name
        self.required_col = list(inspect.getfullargspec(self.func))[0]

        # Please notice if you are implementing a new callback, we only check if the package(s) are installed.
        # You should be the one who actually import the package(s) within your callback class.
        have_pkg = [importlib.util.find_spec(pkg) is not None for pkg in required_pkgs]
        print(have_pkg)
        if not all(have_pkg):
            raise ImportError(
                f"Package(s) {list(compress(required_pkgs, np.invert(have_pkg)))} are required to use this callback"
            )

    @abstractmethod
    def func(self, *args, **kwargs):
        """
        Function to be called to get the new column value
        """
        pass


class ZeroPointCallback(QueryCallback):
    """
    Callback to use ``gaiadr3_zeropoint`` to get zero-point corrected parallax

    Parameters
    ----------
    new_col_name : str, optional (default="parallax_w_zp")
        Name of the new column you wan to add
    """

    def __init__(self, new_col_name: str = "parallax_w_zp"):
        super().__init__(new_col_name, required_pkgs=["gaiadr3-zeropoint"])

        self.zpt = importlib.import_module("zero_point.zpt")
        self.zpt.load_tables()

    def func(
        self,
        ra,
        dec,
        parallax,
        phot_bp_mean_mag,
        nu_eff_used_in_astrometry,
        pseudocolour,
        astrometric_params_solved,
    ):
        ect_lon, ect_lat = radec_to_ecl(ra, dec)
        with warnings.catch_warnings(record=True):
            warnings.filterwarnings(action="ignore")
            return parallax - self.zpt.get_zpt(
                phot_bp_mean_mag,
                nu_eff_used_in_astrometry,
                pseudocolour,
                ect_lat,
                astrometric_params_solved,
            )


class DustCallback(QueryCallback):
    """
    Callback to use ``mwdust`` to get extinction

    Parameters
    ----------
    new_col_name : str, optional (default="sfd_ebv")
        Name of the new column you wan to add
    filter : str, optional (default=None)
        extinction in which filter, see mwdust
    dustmap : str, optional (default="SFD")
        which dust map to use, currently only supporting ``SFD``
    """

    def __init__(
        self,
        new_col_name: str = "sfd_ebv",
        filter: Optional[str] = None,
        dustmap: str = "SFD",
    ):
        super().__init__(new_col_name, required_pkgs=["mwdust", "galpy"])
        self.mwdust = importlib.import_module("mwdust")
        self.radec_to_lb = importlib.import_module("galpy.util.coords").radec_to_lb
        self.filter = filter
        if dustmap.lower() == "sfd":
            self.sfd = self.mwdust.SFD(filter=self.filter, noloop=True)
            self._func = self.sfd_ebv_func

    def sfd_ebv_func(self, ra, dec):
        lb = self.radec_to_lb(ra, dec, degree=True)
        return self.sfd(lb[:, 0], lb[:, 1], np.ones_like(lb[:, 0]))

    def func(self, ra, dec):
        return self._func(ra, dec)


class OrbitsCallback(QueryCallback):
    """
    Callback to use ``galpy`` to setup orbit to get orbital parameters

    Parameters
    ----------
    new_col_name : str, optional (default="e")
        Name of the new column you wan to add
    """

    def __init__(self, new_col_name: str = "e"):
        super().__init__(new_col_name, required_pkgs=["galpy"])

        self._r0 = 8.23  # kpc
        self._v0 = 249.44  # km/s
        self._z0 = 0.0208  # kpc

        self.orbit = importlib.import_module("galpy.orbit")
        self.radec_to_lb = importlib.import_module("galpy.util.coords").radec_to_lb

    def sfd_ebv_func(self, ra, dec):
        lb = self.radec_to_lb(ra, dec, degree=True)
        return self.sfd(lb[:, 0], lb[:, 1], np.ones_like(lb[:, 0]))

    def func(self, ra, dec, pmra, pmdec, parallax, radial_velocity):
        return self.orbit.Orbit(
            [
                ra,
                dec,
                (1 / parallax),
                pmra,
                pmdec,
                radial_velocity,
            ],
            radec=True,
            ro=self._r0,
            vo=self._v0,
            zo=self._z0,
        )
