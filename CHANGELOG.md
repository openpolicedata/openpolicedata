# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Added
- Data standardization: Added function for standardizing some column names and data values
### Changed
### Deprecated
### Removed
### Fixed
### Security

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