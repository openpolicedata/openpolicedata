[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
The OpenPoliceData (OPD) Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 500+ incident-level datasets for about 4865 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. 

Users request data by department name and type of data, and the data is returned as a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). There is no need to manually find the data online or to know how to work with open data APIs (ArcGIS, Socrata, etc.). When data is loaded by OPD, the returned data is unmodified (with the exception of formatting known date fields) from what appears on the source's site, and OPD provides links to the original data for transparency.

OpenPoliceData can be installed from the Python Package Index (PyPI):
```
pip install openpolicedata
``` 

OpenPoliceData provides access to police data with 2 simple lines of code:
```
> import openpolicedata as opd
> src = opd.Source("New Orleans")
> data = src.load(table_type="USE OF FORCE", year=2022)
```

- Documentation: https://openpolicedata.readthedocs.io/
- Source Code: https://github.com/openpolicedata/openpolicedata
- Bug Tracker: https://github.com/openpolicedata/openpolicedata/issues
- [Latest Datasets](#latest-datasets-added)
- [Release Notes](#release-notes-for-version-057-2023-09-05)
- [Contributing](#contributing)


![alt text](https://github.com/openpolicedata/opd-data/blob/main/OPD_Datasets_Map.png?raw=true)

## Latest Datasets Added to OPD
- California: Stops data for all departments submitted for RIPA for 2018-2022
- Mapping Police Violence police killings
- Washington Post police killings
- Indiana: Officer-Involved Shootings
- Pittsburgh, PA: Complaints, traffic stops, and use of force
- Fort Worth, TX: Traffic stops and use of force
- Yakima County, WA: Use of force
- Washington D.C.: Historical (2010-2017) stops data
- South Bend, IN: Complaints involving administrative investigations
- Tucson, AZ: Added additional years of arrests

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

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md

## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](mailto:openpolicedata@gmail.com).