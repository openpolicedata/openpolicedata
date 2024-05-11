# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Added
- Added POINTING WEAPON (by officer) table type
- Added data loader to combine multiple files that span a single year into a single dataset
- Added support for more text date column formats in Arcgis loader.
- Added url_contains input to get_count, load_iter, load, and load_from_csv of Source class to distinguish between multiple datasets matching a data request
- Added datasets input to get_years to allow getting the years in specific datasets.
- Added [Year Filter Guide](https://openpolicedata.readthedocs.io/en/stable/getting_started/year_filtering.html) to documentation
### Changed
- Updates to standardization to handle more datasets
### Deprecated
### Removed
### Fixed
- Fixed year filtering for Tucson OFFICER-INVOLVED SHOOTINGS - INCIDENTS dataset. Datasets is no longer available using OpenPoliceData prior to Version 0.7.
### Security

## v0.6 - 2024-05-10
### Added
- Added POINTING WEAPON (by officer) table type
- Added data loader to combine multiple files that span a single year into a single dataset
- Added support for more text date column formats in Arcgis loader.
- Added url_contains input to get_count, load_iter, load, and load_from_csv of Source class to distinguish between multiple datasets matching a data request
- Added datasets input to get_years to allow getting the years in specific datasets.
- Added [Year Filter Guide](https://openpolicedata.readthedocs.io/en/stable/getting_started/year_filtering.html) to documentation
### Changed
- Updates to standardization to handle more datasets
### Fixed
- Fixed year filtering for Tucson OFFICER-INVOLVED SHOOTINGS - INCIDENTS dataset. Datasets is no longer available using OpenPoliceData prior to Version 0.7.

## v0.6 - 2024-02-14
### Added
- Data standardization: Added function for standardizing some column names and data values
- Added reload function to datasets module to allow reloading the datasets table (in case of an update) or loading a datasets table from a custom location
- Added functions for getting race, gender, and age columns after standardization
- Added merge function for merging 2 table together
- Added function for finding related tables
- Added a function for expanding rows that contain information on multiple officers or subjects into multiple row
- Made opd.defs.TableType and opd.defs.columns available as opd.TableType and opd.Column
- Added Table.urls to enable quick retrieval of URLs associated with a dataset
- Added verbose mode to enable transparency when loading data with get_count, load_data_from_url, and load_from_url_gen
- Added Source.load_iter to be used instead of Source.load_from_url_gen
- Added Source.load to be used instead of Source.load_from_url
- Added data loader for CKAN API
### Changed
- Inputs to Source.get_count is now (table_type, year, ...) instead of (year, table_type, ...) so inputs go from general to specific. Original input order is deprecated and will be removed in Version 1.0.
### Deprecated
- Deprecated Source.load_from_url_gen. Will be removed in Version 1.0
- Deprecated Source.load_from_url. Will be removed in Version 1.0
### Removed
- Removed support for Python 3.7 which has reached end of life: https://www.python.org/downloads/release/python-370/
### Fixed
- Improved speed and feedback when reading large CSV files contained in zip files
- Source.get_agencies with a partial_name is now case-insensitive

## v0.5.8 - 2023-09-28
### Added
- Using User-Agent to enable inclusion of Chicago pedestrian stops data
### Changed
- Improved speed of datetime conversion
### Fixed
- Fixed issue when using load_from_url_gen with Socrata and Carto data where data needed to be sorted by data IDs in order to prevent loading a few repeated rows due to changing order of data on server
- Fixed error thrown when dataset has date field but that column is not returned because the rows requested are all empty

## v0.5.7 - 2023-09-05
### Added
- Subclassing pandas DataFrame and Series when appropriate to enable deprecation messaging of certain Table Type names
- Documentation is now available at https://openpolicedata.readthedocs.io/
- Now require specification of Excel worksheets when loading Excel notebooks where the location of the data is unclear
### Changed
- An empty DataFrame is now returned when the request contains no data. Previously, None was returned.
### Deprecated
- The word *CIVILIAN* in table type names has been replaced by *SUBJECT* to be consistent with the standard set by the [Stanford Open Policing Project](https://github.com/stanford-policylab/opp/blob/master/data_readme.md).
### Removed
### Fixed
- Fixed date filtering in CSV and Excel data loaders
### Security

## Previous Versions
A Changelog was not kept prior to Version 0.5.7