[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
The OpenPoliceData (OPD) Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 425+ incident-level datasets for about 4850 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. 

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

> **NEW STARTING IN VERSION 0.6**: OPD now provides tools for automated data standardization. Applying these tools allow you to start your analysis more quickly by replacing column names and data with standard values for some common column types. [Learn how it works and how to use it here.](https://openpolicedata.readthedocs.io/en/stable/getting_started/index.html#Data-Standardization)

- Documentation: https://openpolicedata.readthedocs.io/
- Source Code: https://github.com/openpolicedata/openpolicedata
- Bug Tracker: https://github.com/openpolicedata/openpolicedata/issues
- [Latest Datasets](#latest-datasets-added)
- [Release Notes](#release-notes-for-version-057-2023-09-05)
- [Contributing](#contributing)


![alt text](https://github.com/openpolicedata/opd-data/blob/main/OPD_Datasets_Map.png?raw=true)

## Latest Datasets Added to OPD
- Austin, TX: Arrests, incidents, searches, use of force, and warnings
- New York, NY: Latest pedestrian stops dataset
- Chicago, IL: Latest pedestrian stops dataset
- Washington D.C.: Latest use of force dataset
- Oakland, CA: Latest use of force dataset
- Los Angeles, CA: Latest stops dataset
- Columbia, MO: Latest traffic stops dataset
- Sparks, NV: Latest officer-involved shootings dataset
- Wallkill, NY: Latest stops dataset
- Charleston, SC: Latest citations dataset
- Denver, CO: Crashes
- Mesa, AZ: Latest calls for service dataset
- Wichita, KS: Latest crashes datasets
- Baltimore, MD: Added calls for service datasets
- Pittsfield, MA: Latests arrests, calls for service, and crashes datasets
- Minnesapolis, MN: Incidents
- St. Paul, MN: Incidents
- Durham, NC: Latest calls for service dataset
- Philadelphia, PA: Latests crashes dataset

## v0.7.2 - 2024-07-13
### Added
- Added id_contains input to get_count, load_iter, load, and load_from_csv of Source class to help distinguish between multiple datasets matching a data request (along with previously added url_contains)
- Added SEARCHES, WARNINGS, STOPS_INCIDENTS, and STOPS_SUBJECTS table types

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md

## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](mailto:openpolicedata@gmail.com).