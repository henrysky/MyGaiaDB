import subprocess
from mygaiadb.data import (
    downloader,
    _GAIA_DR3_GAIASOURCE_PARENT,
    _GAIA_DR3_ASTROPHYS_PARENT,
    _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT,
    _GAIA_DR3_2MASS_NEIGHBOUR_PARENT,
    _GAIA_DR3_XP_CONTINUOUS_PARENT,
    _GAIA_DR3_XP_SAMPLED_PARENT,
    _GAIA_DR3_RVS_PARENT,
    _ALLWISE_PARENT,
    _2MASS_PARENT,
    _CATWISE_PARENT,
)


def download_gaia_source(test: bool=False):
    """
    Download Gaia DR3 source data from the ESA Gaia archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_GAIASOURCE_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/"
    if test:
        downloader(
            f"{_url}GaiaSource_000000-003111.csv.gz",
            _GAIA_DR3_GAIASOURCE_PARENT.joinpath("GaiaSource_000000-003111.csv.gz"),
            "test",
            test=test,
        )
        downloader(
            f"{_url}GaiaSource_003112-005263.csv.gz",
            _GAIA_DR3_GAIASOURCE_PARENT.joinpath("GaiaSource_003112-005263.csv.gz"),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_GAIASOURCE_PARENT.as_posix()} && curl --no-clobber -s -O {_url}GaiaSource_000000-003111.csv.gz"
        # subprocess.run(cmd_str, shell=True)
        # cmd_str = f"cd {_GAIA_DR3_GAIASOURCE_PARENT.as_posix()} && curl --no-clobber -s -O {_url}GaiaSource_003112-005263.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_GAIASOURCE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_gaia_astrophysical_parameters(test: bool=False):
    """
    Download Gaia DR3 astrophysical parameters data from the ESA Gaia archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_ASTROPHYS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Astrophysical_parameters/astrophysical_parameters/"
    if test:
        downloader(
            f"{_url}AstrophysicalParameters_000000-003111.csv.gz",
            _GAIA_DR3_ASTROPHYS_PARENT.joinpath(
                "AstrophysicalParameters_000000-003111.csv.gz"
            ),
            "test",
            test=test,
        )
        downloader(
            f"{_url}AstrophysicalParameters_003112-005263.csv.gz",
            _GAIA_DR3_ASTROPHYS_PARENT.joinpath(
                "AstrophysicalParameters_003112-005263.csv.gz"
            ),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_ASTROPHYS_PARENT.as_posix()} && curl --no-clobber -s -O {_url}AstrophysicalParameters_000000-003111.csv.gz"
        # subprocess.run(cmd_str, shell=True)
        # cmd_str = f"cd {_GAIA_DR3_ASTROPHYS_PARENT.as_posix()} && curl --no-clobber -s -O {_url}AstrophysicalParameters_003112-005263.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_ASTROPHYS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_allwise_best_neightbour(test: bool=False):
    """
    Download AllWISE best neighbour data from the ESA Gaia archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gedr3/cross_match/allwise_best_neighbour/"
    if test:
        downloader(
            f"{_url}allwiseBestNeighbour0001.csv.gz",
            _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.joinpath(
                "allwiseBestNeighbour0001.csv.gz"
            ),
            "test",
            test=test,
        )
        downloader(
            f"{_url}allwiseBestNeighbour0002.csv.gz",
            _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.joinpath(
                "allwiseBestNeighbour0002.csv.gz"
            ),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.as_posix()} && curl --no-clobber -s -O {_url}allwiseBestNeighbour0001.csv.gz"
        # subprocess.run(cmd_str, shell=True)
        # cmd_str = f"cd {_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.as_posix()} && curl --no-clobber -s -O {_url}allwiseBestNeighbour0002.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_2mass_best_neightbour(test: bool=False):
    """
    Download 2MASS best neighbour data from the ESA Gaia archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_2MASS_NEIGHBOUR_PARENT.mkdir(parents=True, exist_ok=True)
    _url = (
        "http://cdn.gea.esac.esa.int/Gaia/gedr3/cross_match/tmasspscxsc_best_neighbour/"
    )
    if test:
        downloader(
            f"{_url}tmasspscxscBestNeighbour0001.csv.gz",
            _GAIA_DR3_2MASS_NEIGHBOUR_PARENT.joinpath(
                "tmasspscxscBestNeighbour0001.csv.gz"
            ),
            "test",
            test=test,
        )
        downloader(
            f"{_url}tmasspscxscBestNeighbour0002.csv.gz",
            _GAIA_DR3_2MASS_NEIGHBOUR_PARENT.joinpath(
                "tmasspscxscBestNeighbour0002.csv.gz"
            ),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_2MASS_NEIGHBOUR_PARENT.as_posix()} && curl --no-clobber -s -O {_url}tmasspscxscBestNeighbour0001.csv.gz"
        # subprocess.run(cmd_str, shell=True)
        # cmd_str = f"cd {_GAIA_DR3_2MASS_NEIGHBOUR_PARENT.as_posix()} && curl --no-clobber -s -O {_url}tmasspscxscBestNeighbour0002.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_2MASS_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_2mass(test: bool=False):
    """
    Download 2MASS data from the IPAC archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _2MASS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "https://irsa.ipac.caltech.edu/2MASS/download/allsky/"
    if test:
        downloader(
            f"{_url}psc_aaa.gz", _2MASS_PARENT.joinpath("psc_aaa.gz"), "test", test=test
        )
        downloader(
            f"{_url}psc_aab.gz", _2MASS_PARENT.joinpath("psc_aab.gz"), "test", test=test
        )
        # cmd_str = f"cd {_2MASS_PARENT.as_posix()} && curl --no-clobber -s -O {_url}psc_aaa.gz"
        # subprocess.run(cmd_str, shell=True)
        # cmd_str = f"cd {_2MASS_PARENT.as_posix()} && curl --no-clobber -s -O {_url}psc_aab.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_2MASS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_allwise(test: bool=False):
    """
    Download AllWISE data from the IPAC archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _ALLWISE_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://irsa.ipac.caltech.edu/data/download/wise-allwise/"
    # https://irsa.ipac.caltech.edu/data/download/wise-allwise/wget_bz2.script
    cmd_list = [
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/README.txt",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat.bz2.sizes.txt",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat.bz2.md5.txt",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part01.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part02.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part03.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part04.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part05.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part06.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part07.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part08.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part09.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part10.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part11.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part12.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part13.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part14.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part15.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part16.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part17.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part18.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part19.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part20.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part21.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part22.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part23.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part24.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part25.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part26.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part27.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part28.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part29.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part30.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part31.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part32.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part33.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part34.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part35.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part36.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part37.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part38.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part39.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part40.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part41.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part42.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part43.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part44.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part45.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part46.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part47.bz2",
        f"wget -P {_ALLWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part48.bz2",
    ]

    if test:
        downloader(
            "http://irsa.ipac.caltech.edu/data/download/wise-allwise/wise-allwise-cat-part01.bz2",
            _ALLWISE_PARENT.joinpath("wise-allwise-cat-part01.bz2"),
            "test",
            test=test,
        )
        # subprocess.run(cmd_list[3], shell=True)
    else:
        for cmd_str in cmd_list:
            subprocess.run(cmd_str, shell=True)


def download_gaia_xp_continuous(test: bool=False):
    """
    Download Gaia DR3 XP continuous spectra from the ESA Gaia archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_XP_CONTINUOUS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_continuous_mean_spectrum/"
    if test:
        downloader(
            f"{_url}XpContinuousMeanSpectrum_000000-003111.csv.gz",
            _GAIA_DR3_XP_CONTINUOUS_PARENT.joinpath(
                "XpContinuousMeanSpectrum_000000-003111.csv.gz"
            ),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_XP_CONTINUOUS_PARENT.as_posix()} && curl --no-clobber -s -O {_url}XpContinuousMeanSpectrum_000000-003111.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_XP_CONTINUOUS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_gaia_xp_sampled(test: bool=False):
    """
    Download Gaia DR3 XP sampled spectra from the ESA Gaia archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_XP_SAMPLED_PARENT.mkdir(parents=True, exist_ok=True)
    _url = (
        "http://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_sampled_mean_spectrum/"
    )
    if test:
        downloader(
            f"{_url}XpSampledMeanSpectrum_000000-003111.csv.gz",
            _GAIA_DR3_XP_SAMPLED_PARENT.joinpath(
                "XpSampledMeanSpectrum_000000-003111.csv.gz"
            ),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_XP_SAMPLED_PARENT.as_posix()} && curl --no-clobber -s -O {_url}XpSampledMeanSpectrum_000000-003111.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_XP_SAMPLED_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_gaia_rvs(test: bool=False):
    """
    Download Gaia DR3 RVS spectra from the ESA Gaia archive.
    
    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _GAIA_DR3_RVS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/rvs_mean_spectrum/"
    if test:
        downloader(
            f"{_url}RvsMeanSpectrum_000000-003111.csv.gz",
            _GAIA_DR3_RVS_PARENT.joinpath("RvsMeanSpectrum_000000-003111.csv.gz"),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_GAIA_DR3_RVS_PARENT.as_posix()} && curl --no-clobber -s -O {_url}RvsMeanSpectrum_000000-003111.csv.gz"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_RVS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_catwise(test: bool=False):
    """
    Download CatWISE data from the NERSC archive.

    Parameters
    ----------
    test : bool, optional (default=False)
        If True, only download a small subset of the data for testing purposes.
    """
    _CATWISE_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/"
    if test:
        downloader(
            f"{_url}000/0000m016_opt1_20191208_213403_ab_v5_cat_b0.tbl.gz",
            _CATWISE_PARENT.joinpath(
                "000", "0000m016_opt1_20191208_213403_ab_v5_cat_b0.tbl.gz"
            ),
            "test",
            test=test,
        )
        # cmd_str = f"cd {_CATWISE_PARENT.as_posix()} && curl --no-clobber -s -O {_url}catwise_2020a.tbl"
        # subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_CATWISE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive -R 'index.html*' --level=2 -e robots=off --no-host-directories --cut-dirs=5 {_url}"
        subprocess.run(cmd_str, shell=True)
