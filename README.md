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
- Louisville, KY: Officer-involved shootings data
- Norwich, CT: Officer-involved shootings data
- Fairfax County, VA: 2023 arrests, traffic citations, and traffic warnings
- Asheville, NC: arrests, citations, complaints, incidents, pointing weapon, traffic stops, use of force, and 2023 calls for service
- Sacramento, CA: 2024 calls for service, 2021-2024 incidents, and 2023-2024 citations   
- Albemarle County, VA: Stops
- Norman, OK: Crashes, incidents, and traffic stops data (new) and most recent arrests, complaints and use of force data
- Oakland, CA: Stops
- Washington D.C.: Lawsuits against MPD
- Bloomington, IN: Use of Force and Citations

## v0.7.1 - 2024-06-01
### Added
- Added subject and officer name to standardization fields for officer-involved shootings only
- Added capability to handle more formats for storing data in Excel files (different ways of storing data across sheets and multiple tables in a single sheet)
### Changed
- Year column in the OPD source table has been moved to the left to make it more visible
### Deprecated
- Usage of iloc with datasets table is now discouraged due to change in location of Year column

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md

## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](mailto:openpolicedata@gmail.com).