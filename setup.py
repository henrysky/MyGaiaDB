import os
from setuptools import setup, find_packages

with open(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.rst"),
    encoding="utf-8",
) as f:
    long_description = f.read()

setup(
    name="MyGaiaDB",
    version="0.1",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Astronomy",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Database :: Front-Ends",
    ],
    packages=find_packages(),
    package_data={"mygaiadb/sql_schema": ["*"]},
    include_package_data=True,
    python_requires=f">=3.8",
    install_requires=[
        "numpy",
        "astropy",
        "h5py",
        "pandas",
        "tqdm",
    ],
    url="https://github.com/henrysky/MyGaiaDB",
    project_urls={
        "Bug Tracker": "https://github.com/henrysky/MyGaiaDB/issues",
        "Documentation": "https://github.com/henrysky/MyGaiaDB",
        "Source Code": "https://github.com/henrysky/MyGaiaDB",
    },
    license="MIT",
    author="Henry Leung",
    author_email="henrysky.leung@utoronto.ca",
    description="Setup local serverless ESA Gaia / 2MASS / ALLWISE databases and run query locally with python",
    long_description=long_description,
)
