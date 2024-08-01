import pathlib
import shutil
from typing import Optional

import requests
import tqdm

from mygaiadb import astro_data_path

_GAIA_PARENT = astro_data_path.joinpath("gaia_mirror")
_GAIA_DR3_PARENT = _GAIA_PARENT.joinpath("Gaia", "gdr3")
_GAIA_DR3_GAIASOURCE_PARENT = _GAIA_DR3_PARENT.joinpath("gaia_source")
_GAIA_DR3_ASTROPHYS_PARENT = _GAIA_DR3_PARENT.joinpath(
    "Astrophysical_parameters", "astrophysical_parameters"
)
_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT = _GAIA_DR3_PARENT.joinpath(
    "cross_match", "allwise_best_neighbour"
)
_GAIA_DR3_2MASS_NEIGHBOUR_PARENT = _GAIA_DR3_PARENT.joinpath(
    "cross_match", "tmasspscxsc_best_neighbour"
)
_GAIA_DR3_XP_CONTINUOUS_PARENT = _GAIA_DR3_PARENT.joinpath(
    "Spectroscopy", "xp_continuous_mean_spectrum"
)
_GAIA_DR3_XP_SAMPLED_PARENT = _GAIA_DR3_PARENT.joinpath(
    "Spectroscopy", "xp_sampled_mean_spectrum"
)
_GAIA_DR3_RVS_PARENT = _GAIA_DR3_PARENT.joinpath("Spectroscopy", "rvs_mean_spectrum")
_2MASS_PARENT = astro_data_path.joinpath("2mass_mirror")
_ALLWISE_PARENT = astro_data_path.joinpath("allwise_mirror")
_CATWISE_PARENT = astro_data_path.joinpath("catwise_mirror", "2020")


def downloader(
    url: str,
    fullfilename: str,
    name: str,
    test: bool = False,
    session: Optional[requests.Session] = None,
):
    """
    Download a file from a URL to a local file using ``requests``

    Parameters
    ----------
    url : str
        URL of the file to download
    fullfilename : str
        Full path to the local file to save
    name : str
        Name of the file to download
    test : bool, optional
        If True, the file will not be downloaded if it already exists
    session : requests.Session, optional
        A requests session to use for the download. If None, a new session will be created
    """
    if session is None:
        s = requests.Session()
    user_agent = "Mozilla/5.0"
    r = s.get(
        url,
        stream=True,
        allow_redirects=True,
        verify=True,
        headers={"User-Agent": user_agent},
    )
    if r.status_code == 404:
        raise ConnectionError(f"Cannot find {name} data file at {url}")
    r.raise_for_status()  # Will only raise for 4xx codes\
    path = pathlib.Path(fullfilename).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not test or not path.exists():
        file_size = int(r.headers.get("Content-Length", 0))
        # r.raw.read
        with tqdm.tqdm.wrapattr(
            r.raw, "read", total=file_size, desc=f"Download {name}"
        ) as r_raw:
            with path.open("wb") as f:
                shutil.copyfileobj(r_raw, f)
