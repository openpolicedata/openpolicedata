[![PyPI version](https://badge.fury.io/py/openpolicedata.svg)](https://badge.fury.io/py/openpolicedata)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/openpolicedata/opd-examples/HEAD)

# OpenPoliceData
OpenPoliceData is a pandas-based Python package for police data analysis that provides easy access to incident-level data from police departments around the United States for traffic stops, pedestrian stops, use of force, and other types of police interactions.

**[Installation](#installation)**

**[Examples](#examples)**

**[Contributing](#contributing)**

**[Querying Available Data](#querying-available-data)**

**[Loading and Working with Datasets](#loading-and-working-with-datasets)**

## Installation
The source code is available at https://github.com/openpolicedata/openpolicedata.

OpenPoliceData can be installed from the Python Package Index (PyPI):
```
pip install openpolicedata
``` 

Additionally, [geopandas](https://geopandas.org/en/stable/getting_started/install.html) can be installed to enable downloaded data tables to be returned as geopandas DataFrames instead of pandas DataFrames when there is geographic data. It is recommended to use [conda](https://docs.conda.io/en/latest/) to install geopandas.

## Examples
[Jupyter notebooks](https://jupyter.org/) demonstrating example usage of OpenPoliceData can be found in the [opd-examples](https://github.com/openpolicedata/opd-examples) repo. 

**[Try OpenPoliceData online on Binder](https://github.com/openpolicedata/opd-examples).**

## Contributing
If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD)

## Import
```
> import openpolicedata as opd
```

## Querying Available Data
### opd.datasets.query(source_name=None, state=None, agency=None, table_type=None)
Query the available datasets to see what is available. Various filters can be applied. By default, all datasets are returned.
```
> datasets = opd.datasets.query(state="California")
> datasets.head()
```
| **State**  | **SourceName** | **Agency** | **TableType** | **Year** |
|------------|----------------|------------------|---------------|----------|
| California | Anaheim        | Anaheim          | TRAFFIC STOPS | MULTI    |
| California | Bakersfield    | Bakersfield      | TRAFFIC STOPS | MULTI    |
| California | California     | MULTI            | STOPS         | 2018     |
| California | California     | MULTI            | STOPS         | 2019     |
| California | California     | MULTI            | STOPS         | 2020     |

(only 1st 5 columns shown above)

datasets is a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). The first 5 datasets available from California include traffic stops data from multiples years from Anaheim and Bakersfield and data from every agency in California for all types of police stops for years 2018, 2019, and 2020.

### opd.datasets.num_unique()
Returns the number of unique datasets in OpenPoliceData. This counts the number of datasets from distinct sources AND table types (stops, use of force, etc.).

### opd.datasets.num_sources(full_states_only=False)
Returns the number of sources (police departments and states) that provide the data available in OpenPoliceData. Setting `full_states_only` to True returns only the number of states that share data for all agencies in the state.

### opd.datasets.summary_by_state(by=None)
Returns a pandas DataFrame with the number of datasets available for each state. The optional input `by` can be used to further breakdown by "year" or "table".

### opd.datasets.summary_by_table_type(by_year=False)
Returns a pandas DataFrame with the number of datasets available for each type of table (stops, use of force, etc.). Setting `by_year` to True also returns a breakdown of table types by year.

## Loading and Working with Datasets
### opd.Source(source_name, state=None)
Create a data source. A data source allows the user to easily import or export police data. It provides access to all datasets available from a source. `source_name` should match a value of SourceName for an available dataset. An optional `state` parameter is used to resolve ambiguities when the same source name is used in multiple states (such as multiple states have State Police).
```
> src = opd.Source(source_name="Virginia")
> src.datasets
```
| **State**  | **SourceName** | **Agency** | **TableType** | **Year** |
|------------|----------------|------------------|---------------|----------|
| Virginia   | Virginia       | MULTI            | STOPS         | MULTI    |

(only 1st 5 columns shown above)

There is 1 dataset available from the state of Virginia that contains data from every agency in Virginia for all types of police stops for multiple years.

### get_tables_types()
Show all types of data available from a source.
```
> src.get_tables_types()
['STOPS']
```

### get_years(table_type=None)
Show years available for one or more datasets. Results can be filtered to only show years for a specific type of data.
```
> src.get_years(table_type="STOPS")
[2020, 2021, 2022]
```

### get_agencies(table_type=None, year=None, partial_name=None)
Show agencies (police departments) that have data available. This is typically a single agency unless the data is from a state. Results can be filtered to only show agencies for a specific type of data and/or year. `partial_name` can be used to find only agencies containing a substring. This is useful for finding the exact name of a police department.
```
> agencies = src.get_agencies(partial_name="Arlington")
> print(agencies)
['Arlington County Police Department', "Arlington County Sheriff's Office"]
```
### load_from_url(year, table_type=None, agency=None, pbar=True)
Import data from the source. Data for a year (i.e. 2020) or a range of years (i.e. [2020, 2022]) can be requested. If more than one data type is available, `table_type` must be specified. Optionally, for datasets containing multiple agencies (police departments) data, `agency` can be used to request data for a single agency. `pbar` can be set to false to not show a progress bar while loading.
```
> agency = "Arlington County Police Department"
> tbl = src.load_from_url(year=2021, table_type="STOPS", agency=agency)
> tbl.table.head(n=3)
```

| **incident_date**  | **agency_name** | **agency** | **reason_for_stop** | **race** | **ethnicity** |
|------------|----------------|------------------|---------------|----------|----------|
| 2021-01-01 | Arlington County Police Department        | ARLINGTON CO          | OTHER | WHITE    | HISPANIC    |
| 2021-01-01 | Arlington County Police Department    | ARLINGTON CO      | EQUIPMENT VIOLATION | WHITE    | NON-HISPANIC    |
| 2021-01-01 | Arlington County Police Department    | ARLINGTON CO      | TRAFFIC VIOLATION | BLACK OR AFRICAN AMERICAN    | NON-HISPANIC    |

(only 1st 6 columns shown above)

The result of load_from_url is a Table object. The table contained in the Table object is either a [geopandas](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html) or [pandas](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) DataFrame depending on whether the returned data contains geographic data or not.

### to_csv(output_dir=None, filename=None)
Export table to CSV. The default output directory is the current directory. The default filename is automatically generated which enables the user to easily re-import the table to a new Table object.
```
> tbl.to_csv()
```
### load_from_csv(year, output_dir=None, table_type=None, agency=None)
Import table from previously exported CSV. The directory to look in defaults to the current directory. The CSV file must have been automatically generated (see [to_csv](#tocsvoutputdirnone-filenamenone)). `year`, `table_type`, and `agency` are defined the same as for [load_from_url](#loadfromurlyear-tabletypenone-agencyfilternone).
```
> new_src = opd.Source(source_name="Virginia")
new_t = new_src.load_from_csv(year=2021, agency=agency)


> tbl.table.head(n=3)
```

| **incident_date**  | **agency_name** | **agency** | **reason_for_stop** | **race** | **ethnicity** |
|------------|----------------|------------------|---------------|----------|----------|
| 2021-01-01 | Arlington County Police Department        | ARLINGTON CO          | OTHER | WHITE    | HISPANIC    |
| 2021-01-01 | Arlington County Police Department    | ARLINGTON CO      | EQUIPMENT VIOLATION | WHITE    | NON-HISPANIC    |
| 2021-01-01 | Arlington County Police Department    | ARLINGTON CO      | TRAFFIC VIOLATION | BLACK OR AFRICAN AMERICAN    | NON-HISPANIC    |

(only 1st 6 columns shown above)


## See the [OpenPoliceData wiki](https://github.com/openpolicedata/openpolicedata/wiki) for further documentation
