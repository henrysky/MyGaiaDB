import tqdm
import shutil
import pathlib
import requests
from .. import astro_data_path

_GAIA_PARENT = astro_data_path.joinpath("gaia_mirror")
_GAIA_DR3_PARENT = _GAIA_PARENT.joinpath("Gaia", "gdr3")
_GAIA_DR3_GAIASOURCE_PARENT = _GAIA_DR3_PARENT.joinpath("gaia_source")
_GAIA_DR3_ASTROPHYS_PARENT = _GAIA_DR3_PARENT.joinpath("Astrophysical_parameters", "astrophysical_parameters")
_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT = _GAIA_DR3_PARENT.joinpath("cross_match", "allwise_best_neighbour")
_GAIA_DR3_2MASS_NEIGHBOUR_PARENT = _GAIA_DR3_PARENT.joinpath("cross_match", "tmasspscxsc_best_neighbour")
_GAIA_DR3_XP_CONTINUOUS_PARENT = _GAIA_DR3_PARENT.joinpath("Spectroscopy", "xp_continuous_mean_spectrum")
_GAIA_DR3_XP_SAMPLED_PARENT = _GAIA_DR3_PARENT.joinpath("Spectroscopy", "xp_sampled_mean_spectrum")
_GAIA_DR3_RVS_PARENT = _GAIA_DR3_PARENT.joinpath("Spectroscopy", "rvs_mean_spectrum")
_2MASS_PARENT = astro_data_path.joinpath("2mass_mirror")
_ALLWISE_PARENT = astro_data_path.joinpath("allwise_mirror")
_CATWISE_PARENT = astro_data_path.joinpath("catwise_mirror", "2020")


def downloader(url, fullfilename, name, test=False, session=None):
    """
    url: URL of data file
    fullfilename: full local path
    name: name of the task
    session: Requests session
    """
    if session is None:
        s = requests.Session()
    user_agent = "Mozilla/5.0"
    r = requests.get(url, stream=True, allow_redirects=True, verify=True, headers={"User-Agent": user_agent})
    if r.status_code == 404:
        raise ConnectionError(f"Cannot find {name} data file at {url}")
    r.raise_for_status()  # Will only raise for 4xx codes\
    path = pathlib.Path(fullfilename).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not test or not path.exists():
        file_size = int(r.headers.get('Content-Length', 0))
        # r.raw.read
        with tqdm.tqdm.wrapattr(r.raw, "read", total=file_size, desc=f"Download {name}") as r_raw:
            with path.open("wb") as f:
                shutil.copyfileobj(r_raw, f)
