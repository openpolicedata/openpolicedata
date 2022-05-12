# OpenPoliceData
OpenPoliceData is a Python package for police data analysis that provides easy access to incident-level data from police departments around the United States for traffic stops, pedestrian stops, use of force, and other types of police interactions.

## Installation
OpenPoliceData is easiest to install with ``conda``. Install [miniconda](https://docs.conda.io/en/latest/miniconda.html).

* Clone the repo
> `git clone git@github.com:openpolicedata/openpolicedata.git`
* After cloning the repo change to the directory
> `cd openpolicedata`

* Install the dependencies (add "-n my_env" below and replace my_env with your conda environment to install to an environment other than base)
> `conda env update -f environment.yml`

## Examples
[Jupyter notebooks](https://jupyter.org/) demonstrating example usage of OpenPoliceData can be found in the [notebooks](https://github.com/openpolicedata/openpolicedata/tree/main/notebooks) folder.

## Contributing
If you're interesting in helping out, see our [Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD)

## Documentation
### datasets_query(source_name=None, state=None, agency=None, table_type=None)
Query the available datasets to see what is available. Various filters can be applied. By default, all datasets are returned.
```
> import openpolicedata as opd
> datasets = opd.datasets_query(state="California")
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

### Source(source_name, state=None)
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
### load_from_url(year, table_type=None, agency_filter=None)
Import data from the source. Data for a year (i.e. 2020) or a range of years (i.e. [2020, 2022]) can be requested. If more than one data type is available, `table_type` must be specified. Optionally, for datasets containing multiple agencies (police departments) data, `agency_filter` can be used to request data for a single agency.
```
> agency = "Arlington County Police Department"
> tbl = src.load_from_url(year=2021, table_type="STOPS", agency_filter=agency)
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
### load_from_csv(year, output_dir=None, table_type=None, agency_filter=None)
Import table from previously exported CSV. The directory to look in defaults to the current directory. The CSV file must have been automatically generated (see [to_csv](#tocsvoutputdirnone-filenamenone)). `year`, `table_type`, and `agency_filter` are defined the same as for [load_from_url](#loadfromurlyear-tabletypenone-agencyfilternone).
```
> new_src = opd.Source(source_name="Virginia")
new_t = new_src.load_from_csv(year=2021, agency_filter=agency)
> tbl.table.head(n=3)
```

| **incident_date**  | **agency_name** | **agency** | **reason_for_stop** | **race** | **ethnicity** |
|------------|----------------|------------------|---------------|----------|----------|
| 2021-01-01 | Arlington County Police Department        | ARLINGTON CO          | OTHER | WHITE    | HISPANIC    |
| 2021-01-01 | Arlington County Police Department    | ARLINGTON CO      | EQUIPMENT VIOLATION | WHITE    | NON-HISPANIC    |
| 2021-01-01 | Arlington County Police Department    | ARLINGTON CO      | TRAFFIC VIOLATION | BLACK OR AFRICAN AMERICAN    | NON-HISPANIC    |

(only 1st 6 columns shown above)
