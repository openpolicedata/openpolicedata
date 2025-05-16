[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
The OpenPoliceData (OPD) Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 500+ incident-level datasets from about 233 police agencies and 11 entire states. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. 

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
- Washington D.C.: 2023 Use of Force
- Rochester, NY: Use of Force and Officer-Involved Shootings
- Omaha, NE: Officer-Involved Shootings, Complaints, and Traffic Stops
- Merced, CA: Use of Force and Complaints
- St. Paul, MN: 2023 Citations
- Philadelphia: 2023 Crashes
- California: Stops data for all departments submitted for RIPA for 2018-2022
- Mapping Police Violence police killings
- Washington Post police killings
- Indiana: Officer-Involved Shootings
- Pittsburgh, PA: Complaints, traffic stops, and use of force

## v0.10 - 2025-03-09
### Added
- Added test that all datasets associated with a loader were added after introduction of that loader
- Add Excel loader code to handle complex Omaha officer-involved shootings datasets
- Add query capability to Arcgis loader
### Changed
- Improved removal of non-table data from Excel sheets
- Improved logging when using verbose input in Source and Table methods
- Changed some dataset IDs to be in JSON format instead of custom format

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md

## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](mailto:openpolicedata@gmail.com).