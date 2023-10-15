## [0.5.0] - 202x-xx-xx

### Added
- N/A

### Changed
- N/A

### Fixed
- N/A

## [0.4] - 2023-10-01

### Added
- [0.4.1] Python 3.12 wheels

### Changed
- Fix ``compile_xp_continuous_h5()`` won't run on new computer since it assumed non-standard file structure
- Reduce memory usage for ``compile_xp_continuous_h5()``
- ``compile_xp_continuous_allinone_h5()`` can now save correlation matrix too

### Fixed
- [0.4.1] Fix ``yield_xp_coeffs()`` fails for some gaia source id due to dtype issue

## [0.3] - 2023-08-05

### Added
- Support for CATWISE database
- Option to save original query as comments in csv file
- IPython autocomplete in ``remove_user_table()``
- Option to get additional columns in ``yield_xp_coeffs()``
- Contribution guideline on Github

### Changed
- Improved performance for ``yield_xp_coeffs()``

### Fixed
- N/A

## [0.2] - 2023-04-01

### Added
- SQL C extension for all maths and limited subset of geometrical functions mimicing ADQL
- Compiled wheels on PyPI

### Changed
- N/A

### Fixed
- N/A

## [0.1] - 2023-03-14

### Added
- Initial Release of ``MyGaiaDB``

### Changed
- N/A

### Fixed
- N/A