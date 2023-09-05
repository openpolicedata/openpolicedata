[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
OpenPoliceData is a Python library that provides easy access to 365 (and growing) incident-level open datasets for over 4000 police agencies around the United States. Datasets include traffic stops, use of force, officer-involved shootings, complaints, and other types of police interactions. 

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

## Latest Datasets Added
- Albany, NY Arrests, Calls for Service, Field Contacts, Indcidents, Traffic Citations, and Use of Force
- Asheville, NC 2022 Calls For Service
- Boulder, CO Arrests, Crashes,and Incidents
- California Deaths in Custody
- Chandler, AZ Arrests, Incidents, and Calls for Service
- Chattanooga, TN Complaints
- Lincoln, NE 2023 datasets
- New York City, NY 2022 Pedestrian Stops
- Oakland, CA 2022 Use of Force
- San Diego, CA 2022 Complaints
- San Jose, CA 2022 and 2023 Calls for Service
- Tacoma, WA Complaints, Incidents, and Officer-Involved Shootings

## Release Notes for Version 0.5.7 (2023-09-05)
### Added
- Subclassing pandas DataFrame and Series when appropriate to enable deprecation messaging of certain Table Type names
- Documentation is now available at https://openpolicedata.readthedocs.io/
- Now require specification of Excel worksheets when loading Excel notebooks where the location of the data is unclear
### Changed
- An empty DataFrame is now returned when the request contains no data. Previously, None was returned.
### Deprecated
- The word *CIVILIAN* in table type names has been replaced by *SUBJECT* to be consistent with the standard set by the [Stanford Open Policing Project](https://github.com/stanford-policylab/opp/blob/master/data_readme.md).
### Fixed
- Fixed date filtering in CSV and Excel data loaders

## Recent Datasets Added
- Albany, NY Arrests, Calls for Service, Field Contacts, Indcidents, Traffic Citations, and Use of Force
- Asheville, NC 2022 Calls For Service
- Boulder, CO Arrests, Crashes,and Incidents
- California Deaths in Custody
- Chandler, AZ Arrests, Incidents, and Calls for Service
- Chattanooga, TN Complaints
- Lincoln, NE 2023 datasets
- New York City, NY 2022 Pedestrian Stops
- Oakland, CA 2022 Use of Force
- San Diego, CA 2022 Complaints
- San Jose, CA 2022 and 2023 Calls for Service
- Tacoma, WA Complaints, Incidents, and Officer-Involved Shootings


## Contributing
All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](openpolicedata@gmail.com).