import subprocess
from . import (
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
)


def download_gaia_source(test=False):
    _GAIA_DR3_GAIASOURCE_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/"
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_GAIASOURCE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}GaiaSource_000000-003111.csv.gz"
        subprocess.run(cmd_str, shell=True)
        cmd_str = f"wget -P {_GAIA_DR3_GAIASOURCE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}GaiaSource_003112-005263.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_GAIASOURCE_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_gaia_astrophysical_parameters(test=False):
    _GAIA_DR3_ASTROPHYS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Astrophysical_parameters/astrophysical_parameters/"
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_ASTROPHYS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}AstrophysicalParameters_000000-003111.csv.gz"
        subprocess.run(cmd_str, shell=True)
        cmd_str = f"wget -P {_GAIA_DR3_ASTROPHYS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}AstrophysicalParameters_003112-005263.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_ASTROPHYS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_allwise_best_neightbour(test=False):
    _GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gedr3/cross_match/allwise_best_neighbour/"
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}allwiseBestNeighbour0001.csv.gz"
        subprocess.run(cmd_str, shell=True)
        cmd_str = f"wget -P {_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}allwiseBestNeighbour0002.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_ALLWISE_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_2mass_best_neightbour(test=False):
    _GAIA_DR3_2MASS_NEIGHBOUR_PARENT.mkdir(parents=True, exist_ok=True)
    _url = (
        "http://cdn.gea.esac.esa.int/Gaia/gedr3/cross_match/tmasspscxsc_best_neighbour/"
    )
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_2MASS_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}tmasspscxscBestNeighbour0001.csv.gz"
        subprocess.run(cmd_str, shell=True)
        cmd_str = f"wget -P {_GAIA_DR3_2MASS_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}tmasspscxscBestNeighbour0002.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_2MASS_NEIGHBOUR_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)

def download_2mass(test=False):
    _2MASS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = (
        "https://irsa.ipac.caltech.edu/2MASS/download/allsky/"
    )
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_2MASS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}psc_aaa.gz"
        subprocess.run(cmd_str, shell=True)
        cmd_str = f"wget -P {_2MASS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}psc_aab.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_2MASS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_allwise(test=False):
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
        downloader(_url, "test", "test", test=test)
        subprocess.run(cmd_list[3], shell=True)
    else:
        for cmd_str in cmd_list:
            subprocess.run(cmd_str, shell=True)


def download_gaia_xp_continuous(test=False):
    _GAIA_DR3_XP_CONTINUOUS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_continuous_mean_spectrum/"
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_XP_CONTINUOUS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}XpContinuousMeanSpectrum_000000-003111.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_XP_CONTINUOUS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_gaia_xp_sampled(test=False):
    _GAIA_DR3_XP_SAMPLED_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_sampled_mean_spectrum/"
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_XP_SAMPLED_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}XpSampledMeanSpectrum_000000-003111.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_XP_SAMPLED_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)


def download_gaia_rvs(test=False):
    _GAIA_DR3_RVS_PARENT.mkdir(parents=True, exist_ok=True)
    _url = "http://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/rvs_mean_spectrum/"
    if test:
        downloader(_url, "test", "test", test=test)
        cmd_str = f"wget -P {_GAIA_DR3_RVS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent {_url}RvsMeanSpectrum_000000-003111.csv.gz"
        subprocess.run(cmd_str, shell=True)
    else:
        cmd_str = f"wget -P {_GAIA_DR3_RVS_PARENT.as_posix()} --no-clobber --no-verbose --no-parent --recursive --level=1 --no-directories {_url}"
        subprocess.run(cmd_str, shell=True)
