[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://openpolicedata.streamlit.app)

# OpenPoliceData
The OpenPoliceData (OPD) Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 550+ incident-level datasets from 236 police agencies and 11 entire states. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. 

Users request data by department name and type of data, and the data is returned as a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). There is no need to manually find the data online or to know how to work with open data APIs (ArcGIS, Socrata, etc.). When data is loaded by OPD, the returned data is unmodified (with the exception of formatting known date fields) from what appears on the source's site, and OPD provides links to the original data for transparency.

OpenPoliceData can be installed from the Python Package Index (PyPI):
```
pip install openpolicedata
``` 

OpenPoliceData provides access to police data with 2 simple lines of code:
```
> import openpolicedata as opd
> src = opd.Source("New Orleans")
> data = src.load(table_type="USE OF FORCE", date=2022)
```

- Documentation: https://openpolicedata.readthedocs.io/
- Source Code: https://github.com/openpolicedata/openpolicedata
- Bug Tracker: https://github.com/openpolicedata/openpolicedata/issues
- [Latest Datasets](#latest-datasets-added)
- [Release Notes](#release-notes-for-version-057-2023-09-05)
- [Contributing](#contributing)


![alt text](https://github.com/openpolicedata/opd-data/blob/main/OPD_Datasets_Map.png?raw=true)

## Latest Datasets Added to OPD
**We've added a huge number of datasets recently!**
- 2025 datasets: Phoenix and San Jose Calls for Service, Cedar Lake, IN Arrests, Calls for Service, Traffic Stops, Traffic Citations, Traffic Warnings, and Crashes, Griffith, IN Arrests, Calls for Service, Traffic Stops, Traffic Citations, Traffic Warnings, and Crashes, St. John, IN Arrests, Calls for Service, Traffic Stops, Traffic Citations, Traffic Warnings, and Crashes, Wichita Crashes, Louisville Incidents, New Orleans Calls for Services and Incidents, Minneapolis Incidents, Portland, OR Calls for Service
- 2024 datasets: Oakland Use of Force, Washington D.C. Incidents and Lawsuits, Chicago Pedestrian Stops, Louisville Incidents, New Orleans Calls for Services and Incidents, Baltimore Calls for Service, Pittsfield, MA Arrests and Crashes, Lincoln Calls for Service, Incidents, Traffic Stops, Vehicle Pursuits, and Use of Force, New York City Pedestrian Stops, Asheville Calls for Service, Durham Calls for Service, Norman Arrests, Comlaints, Crashes, Incidents, Traffic Stops and Use of Force, Charleston Citations, Nashville Calls for Service, Albemarle County VA STOPS, Bremerton, WA Arrests, Citations, and Incidents
- California: Stops data for all departments submitted for RIPA for 2018-2023
- Cincinnati Officer Involved Shootings and Use of Force
- Memphis Traffic Citations and Traffic Stops
- New Jersey State Police 2021 Traffic Stops
- Los Angeles Calls for Service and Incidents
- Seattle Arrests
- Portland, OR Arrests and Incidents
- Richmond, CA Arrests

## Contributing
[<img src="https://avatars.githubusercontent.com/u/67804485" alt="Alt Text" width="35" height="35">](https://github.com/sowdm)
[<img src="https://avatars.githubusercontent.com/u/9930263" alt="Alt Text" width="35" height="35">](https://github.com/potto216)
[<img src="https://avatars.githubusercontent.com/u/56132560" alt="Alt Text" width="35" height="35">](https://github.com/minkedup)
[<img src="https://avatars.githubusercontent.com/u/42755301" alt="Alt Text" width="35" height="35">](https://github.com/imrnmzri)
[<img src="https://avatars.githubusercontent.com/u/178184249" alt="Alt Text" width="35" height="35">](https://github.com/Brijeshthummar02)
[<img src="https://avatars.githubusercontent.com/u/142138112" alt="Alt Text" width="35" height="35">](https://github.com/harikrishnatp)
[<img src="https://avatars.githubusercontent.com/u/170487658" alt="Alt Text" width="35" height="35">](https://github.com/gotog11)
[<img src="https://avatars.githubusercontent.com/u/98405259" alt="Alt Text" width="35" height="35">](https://github.com/apancoast)

All contributions are welcome including code enhancments, bug fixes, bug reports, documentation updates, and locating new datasets. If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) or reach out by [email](mailto:openpolicedata@gmail.com).

## v0.12 - 2025-07-27
### Changed
- Name of year input changed to date in Table and Source classes to more clearly reflect that input can handle both year and date ranges
### Deprecated
- Usage of year input changed to date in Table and Source classes. date input should be used instead.

Complete change log available at: https://github.com/openpolicedata/openpolicedata/blob/main/CHANGELOG.md