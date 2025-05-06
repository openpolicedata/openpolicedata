---
myst:
  html_meta:
    "description lang=en": |
      Top-level documentation for OpenPoliceData.
html_theme.sidebar_secondary.remove: true
---

# Troubleshooting
- [Troubleshooting](#troubleshooting)
  - [Dataset Will Not Load](#dataset-will-not-load)
  - [Deprecations](#deprecations)
    - [Deprecation Warnings](#deprecation-warnings)
    - [Current Deprecations](#current-deprecations)
  - [Report Issues](#report-issues)

## Dataset Will Not Load
OpenPoliceData (OPD) accesses data from many different websites, and occasionally, some websites will be unavailable due to temporary maintenance, data being removed, or data being reposted to a new URL. When unavailable datasets are requested, the resulting error often includes an [HTTP error code](https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#4xx_client_errors) such as `404 Not Found` error. OPD tries to catch these errors to provide tips to the user on how to determine if the error is due to an unavailable website:

```
> src = opd.Source('Buffalo')
> # Attempt to load dataset that was removed by Buffalo (now also removed from OPD)
> src.load(table_type='TRAFFIC ARRESTS',year=2023)

openpolicedata.exceptions.OPD_SocrataHTTPError: ('data.buffalony.gov', '5kqt-m62h', '404 Client Error: Not Found', 'There is likely an issue with the website. Open the URL https://data.buffalony.gov/resource/5kqt-m62h.json with a web browser to confirm. See a list of known site outages at https://github.com/openpolicedata/opd-data/blob/main/outages.csv')
```

The error message provides a URL that can be tested in a web browser to determine if the website is down. A link to a list of [known outages](https://github.com/openpolicedata/opd-data/blob/main/outages.csv) is also provided. If you think you've found an outage that is not on this list, [create an issue](https://github.com/openpolicedata/openpolicedata/issues) or [email us](mailto:openpolicedata@gmail.com).

There may be instances where OPD does not catch an error caused by an unavailable website. In this case, it is recommended that the user try to open the dataset's `source_url` and/or `URL` in a web browser to test it:

```
> src = opd.Source('Tucson')
> idx = (src.datasets['TableType']=='ARRESTS') &  (src.datasets['Year']==2017)  # Find data
> src.datasets[idx]['URL'].iloc[0]  
'https://services3.arcgis.com/9coHY2fvuFjG9HQX/arcgis/rest/services/OpenData_PublicSafety/FeatureServer/0'
> src.datasets[idx]['source_url'].iloc[0]
'https://gisdata.tucsonaz.gov/datasets/cotgis::tucson-police-arrests-2017-open-data/about'
```
Please report the issue (whether a website outage or a bug in OPD) by [creating an issue](https://github.com/openpolicedata/openpolicedata/issues) or [emailing us](mailto:openpolicedata@gmail.com).

## Deprecations
When a new version is released, OpenPoliceData (OPD) tries to make changes that are backward compatible. However, occasionally, changes that are not backward compatible are necessary to add new features or make OPD easier to use. 

### Deprecation Warnings
In order to minimize any disruption, OPD will first release versions that indicate what features have been deprecated prior to releasing versions that remove those features. Usage of deprecated features will result in `DeprecationWarning` messages that include instructions on how to update your code:

```
> src = opd.Source('Oakland')
> tbl_dep = src.load_from_url(2023, 'STOPS')
DeprecationWarning: load_from_url is deprecated and will be removed in a future release. Please use load instead. load uses the same inputs except table_type now comes before year.
> tbl = src.load('STOPS', 2023)
> tbl.table.equals(tbl_dep.table)
True
```

### Current Deprecations
There are currently no deprecated features. All previously deprecated functionality has been removed in version 1.0.

## Report Issues
Report issues with OPD by [creating an issue](https://github.com/openpolicedata/openpolicedata/issues) or [emailing us](mailto:openpolicedata@gmail.com).