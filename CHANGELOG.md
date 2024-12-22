# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Added
- Added test that all datasets associated with a loader were added after introduction of that loader
- Add Excel loader code to handle complex Omaha officer-involved shootings datasets
### Changed
- Improved removal of non-table data from Excel sheets
### Deprecated
### Removed
### Fixed
### Security

## v0.9 - 2024-11-23
### Added
- Added ability to load an Excel file from a zip file containing multiple files
- Adding ability to load multiple CSV files from the same year from a zip file
- Adding ability to handle datasets containing data from multiple states
- Adding ability for CSV files to have a query in the source table to only show relevant police data
- Added minimum Python version to datasets
### Changed
- No longer throwing error if state field is not a known state. Enables multi-state datasets, which will break pre-v0.6 that do not allow for compatibility tables
### Removed
- No longer updating datasets table when data load finds a capitalization error in the date field
### Fixed
- sortby inputs to data loaders now all work
- Fixed issue where input swap checks in Source methods were checking for swaps in the wrong in put.
- Warnings were not being displayed when compatibility source table was being used

## v0.8.1 - 2024-09-02
### Fixed
- Fixed bug in get_count function for CSV files where count was wrong if there were quotes containing new line characters

## v0.8 - 2024-08-24
### Added
- Added data loader for HTML tables
### Changed
- Changed error messages so that more errors in data loading point the user to the list of data site outages
### Fixed
- Fixed bug in CKAN data loader when user requests a range of years

## v0.7.2 - 2024-07-13
### Added
- Added id_contains input to get_count, load_iter, load, and load_from_csv of Source class to help distinguish between multiple datasets matching a data request (along with previously added url_contains)
- Added SEARCHES, WARNINGS, STOPS_INCIDENTS, and STOPS_SUBJECTS table types

## v0.7.1 - 2024-06-01
### Added
- Added subject and officer name to standardization fields for officer-involved shootings only
- Added capability to handle more formats for storing data in Excel files (different ways of storing data across sheets and multiple tables in a single sheet)
### Changed
- Year column in the OPD source table has been moved to the left to make it more visible
### Deprecated
- Usage of iloc with datasets table is now discouraged due to change in location of Year column

## v0.7 - 2024-05-10
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