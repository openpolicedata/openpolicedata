[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
The OpenPoliceData (OPD) Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 383+ incident-level datasets for over 3500 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. 

Users request data by department name and type of data, and the data is returned as a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). There is no need to manually find the data online or to know how to work with open data APIs (ArcGIS, Socrata, etc.).

OpenPoliceData provides access to police data with 2 simple lines of code:
```
> import openpolicedata as opd
> src = opd.Source("New Orleans")
> data = src.load_from_url(year=2022, table_type="USE OF FORCE")
```

- Documentation: https://openpolicedata.readthedocs.io/
- Source Code: https://github.com/openpolicedata/openpolicedata
- Bug Tracker: https://github.com/openpolicedata/openpolicedata/issues
- [Basic Installation](#installation)
- [Latest Datasets](#latest-datasets-added)
- [Release Notes](#release-notes-for-version-057-2023-09-05)
- [Contributing](#contributing)


![alt text](https://github.com/openpolicedata/opd-data/blob/main/OPD_Datasets_Map.png?raw=true)

## Installation
OpenPoliceData can be installed from the Python Package Index (PyPI):
```
pip install openpolicedata
``` 

## Latest Datasets Added to OPD
- Albany, NY: Arrests, Calls for Service, Field Contacts, Indcidents, Traffic Citations, and Use of Force
- Asheville, NC: 2022 Calls For Service
- Boulder, CO: Arrests, Crashes,and Incidents
- California: Deaths in Custody
- Chandler, AZ: Arrests, Incidents, and Calls for Service
- Chattanooga, TN: Complaints
- Chicago, IL: Pedestrian Stops
- Lincoln, NE: 2023 datasets
- New York City, NY: 2022 Pedestrian Stops
- Oakland, CA: 2022 Use of Force
- San Diego, CA: 2022 Complaints
- San Jose, CA: 2022 and 2023 Calls for Service
- Tacoma, WA: Complaints, Incidents, and Officer-Involved Shootings

## Release Notes for Version 0.5.8 - 2023-09-28
### Added
- Using User-Agent to enable inclusion of Chicago pedestrian stops data
### Changed
- Improved speed of datetime conversion
### Fixed
- Fixed issue when using load_from_url_gen with Socrata and Carto data where data needed to be sorted by data IDs in order to prevent loading a few repeated rows due to changing order of data on server
- Fixed error thrown when dataset has date field but that column is not returned because the rows requested are all empty

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md

## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](openpolicedata@gmail.com).