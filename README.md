[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
The OpenPoliceData (OPD) Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 395+ incident-level datasets for about 4800 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. 

Users request data by department name and type of data, and the data is returned as a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). There is no need to manually find the data online or to know how to work with open data APIs (ArcGIS, Socrata, etc.).

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

> **NEW IN VERSION 0.6**: OPD now provides tools for automated data standardization. Applying these tools allow you to start your analysis more quickly by replacing column names and data with standard values for some common column types. [Learn how it works and how to use it here.](https://openpolicedata.readthedocs.io/en/stable/getting_started/index.html#Data-Standardization)

- Documentation: https://openpolicedata.readthedocs.io/
- Source Code: https://github.com/openpolicedata/openpolicedata
- Bug Tracker: https://github.com/openpolicedata/openpolicedata/issues
- [Latest Datasets](#latest-datasets-added)
- [Release Notes](#release-notes-for-version-057-2023-09-05)
- [Contributing](#contributing)


![alt text](https://github.com/openpolicedata/opd-data/blob/main/OPD_Datasets_Map.png?raw=true)

## Latest Datasets Added to OPD
- Chicago, IL: Traffic Citations
- Traffic Citations for agencies across New York State
- Buffalo Crashes and Incidents
- Massachusetts (All Agencies): Employee and Disciplinary Records data
- Alameda County, CA: Incidents
- Marin County, CA: Incidents
- Riverside, CA: Incidents
- Albany, NY: Arrests, Calls for Service, Field Contacts, Incidents, Traffic Citations, and Use of Force
- Chicago, IL: Pedestrian Stops
- New York City, NY: 2022 Pedestrian Stops
- Oakland, CA: 2022 Use of Force
- San Diego, CA: 2022 Complaints
- Tacoma, WA: Calls for Service, Complaints, Incidents, and Officer-Involved Shootings

## Release Notes for Version 0.6 - 2024-02-10
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
### Deprecated
- Deprecated Source.load_from_url_gen. Will be removed in Version 1.0
- Deprecated Source.load_from_url. Will be removed in Version 1.0
### Removed
- Removed support for Python 3.7 which has reached end of life: https://www.python.org/downloads/release/python-370/
### Fixed
- Improved speed and feedback when reading large CSV files contained in zip files

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md

## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](openpolicedata@gmail.com).