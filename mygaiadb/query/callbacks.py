import inspect
import warnings
import numpy as np
from ..utils import radec_to_ecl

try:
    from zero_point import zpt

    _have_zpt = True
except ImportError:
    _have_zpt = False

try:
    import mwdust

    _have_mwdust = True
except ImportError:
    _have_mwdust = False

try:
    from galpy.orbit import Orbit
    from galpy.util.coords import radec_to_lb

    _have_galpy = True
except ImportError:
    _have_galpy = False


class QueryCallback:
    """
    Callback to add new column to SQL query on the fly
    """

    def __init__(self, new_col_name, func):
        """
        INPUT:
            new_col_name (string): Name of the new column you wan to add
            func (function): function that maps query columns to new columns, arguements of this function need to have \
                the same names as columns in query
        """
        self.new_col_name = new_col_name
        self.func = func
        self.required_col = list(inspect.getfullargspec(self.func))[0]


class ZeroPointCallback(QueryCallback):
    def __init__(self, new_col_name="parallax_w_zp"):
        """
        Callback to use ``gaiadr3_zeropoint`` to get zero-point corrected parallax

        INPUT:
            new_col_name (string): Name of the new column you wan to add
        """
        super().__init__(new_col_name, self.parallax_zp_func)
        if not _have_zpt:
            raise RuntimeError(
                "You need to have gaiadr3_zeropoint package installed to use this callback. Please see: https://gitlab.com/icc-ub/public/gaiadr3_zeropoint"
            )
        zpt.load_tables()

    @staticmethod
    def parallax_zp_func(
        ra,
        dec,
        parallax,
        phot_bp_mean_mag,
        nu_eff_used_in_astrometry,
        pseudocolour,
        astrometric_params_solved,
    ):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", UserWarning)
            ect_lon, ect_lat = radec_to_ecl(ra, dec)
            return parallax - zpt.get_zpt(
                phot_bp_mean_mag,
                nu_eff_used_in_astrometry,
                pseudocolour,
                ect_lat,
                astrometric_params_solved,
            )


class DustCallback(QueryCallback):
    def __init__(self, new_col_name="sfd_ebv", filter=None, dustmap="SFD"):
        """
        Callback to use ``mwdust`` to get extinction

        INPUT:
            new_col_name (string): Name of the new column you wan to add
            filter (string): extinction in which filter, see mwdust
            dustmap (string): which dust map to use, currently only supporting "SFD"
        """
        if not _have_mwdust:
            raise RuntimeError(
                "You need to have mwdust package installed to use this callback. Please see: https://github.com/jobovy/mwdust"
            )
        if not _have_galpy:
            raise RuntimeError(
                "You need to have galpy package installed to use this callback. Please see: https://github.com/jobovy/galpy"
            )
        self.filter = filter
        if dustmap.lower() == "sfd":
            self.sfd = mwdust.SFD(filter=self.filter, noloop=True)
            self._func_wrapped = lambda ra, dec: self.sfd_ebv_func(ra, dec)
        super().__init__(new_col_name, self._func_wrapped)

    def sfd_ebv_func(self, ra, dec):
        lb = radec_to_lb(ra, dec, degree=True)
        l, b = lb[:, 0], lb[:, 1]
        return self.sfd(l, b, np.ones_like(l))


class OrbitsCallback(QueryCallback):
    def __init__(self, new_col_name="e"):
        """
        Callback to use ``galpy`` to setup orbit

        INPUT:
            new_col_name (string): Name of the new column you wan to add
            filter (string): extinction in which filter, see mwdust
            dustmap (string): which dust map to use, currently only supporting "SFD"
        """
        if not _have_galpy:
            raise RuntimeError(
                "You need to have galpy package installed to use this callback. Please see: https://github.com/jobovy/galpy"
            )
        _r0 = 8.23
        _v0 = 249.44
        _z0 = 0.0208

        self._func_wrapped = (
            lambda ra, dec, pmra, pmdec, parallax, radial_velocity: Orbit(
                [
                    ra,
                    dec,
                    (1 / parallax),
                    pmra,
                    pmdec,
                    radial_velocity,
                ],
                radec=True,
                ro=_r0,
                vo=_v0,
                zo=_z0,
            )
        )
        super().__init__(new_col_name, self._func_wrapped)

    def sfd_ebv_func(self, ra, dec):
        lb = radec_to_lb(ra, dec, degree=True)
        l, b = lb[:, 0], lb[:, 1]
        return self.sfd(l, b, np.ones_like(l))
