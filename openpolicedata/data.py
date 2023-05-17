import numbers
import os.path as path
import pandas as pd
from datetime import datetime
from dateutil.parser._parser import ParserError
from packaging import version
import re
import warnings

if __name__ == '__main__':
    from datetime_parser import to_datetime
    import data_loaders
    import datasets
    import preproc
    import defs
    from _version import __version__
    import exceptions
else:
    from . import data_loaders
    from . import datasets
    from . import __version__
    from . import preproc
    from . import defs
    from . import exceptions
    from .datetime_parser import to_datetime

class Table:
    """
    A class that contains a DataFrame for a dataset along with meta information

    Attributes
    ----------
    details : pandas Series
        Series containing information about the dataset
    state : str
        Name of state where agencies in table are
    source_name : str
        Name of source
    agency : str
        Name of agency
    table_type : TableType enum
        Type of data contained in table
    year : int, list, MULTI
        Indicates years contained in table
    description : str
        Description of data source
    url : str
        URL where table was accessed from
    table : pandas of geopandasDataFrame
        Data accessed from source

    Methods
    -------
    to_csv(output_dir=None, filename=None)
        Convert table to CSV file
    get_csv_filename()
        Get default name of CSV file
    """

    details = None
    state = None
    source_name = None
    agency = None
    table_type = None
    year = None
    description = None
    url = None

    # Data
    table = None

    # From source
    _data_type = None
    _dataset_id = None
    date_field = None
    agency_field = None
    __transforms = None

    def __init__(self, source, table=None, year_filter=None, agency=None):
        '''Construct Table object
        This is intended to be generated by the Source.load_from_url and Source.load_from_csv classes

        Parameters
        ----------
        source : pandas or geopandas Series
            Series containing information on the source
        table : pandas or geopandas 
            Name of state where agencies in table are
        '''
        if not isinstance(source, pd.core.frame.DataFrame) and \
            not isinstance(source, pd.core.series.Series):
            raise TypeError("data must be an ID, DataFrame or Series")
        elif isinstance(source, pd.core.frame.DataFrame):
            if len(source) == 0:
                raise LookupError("DataFrame is empty")
            elif len(source) > 1:
                raise LookupError("DataFrame has more than 1 row")

            source = source.iloc[0]

        self.details = source
        self.table = table

        self.state = source["State"]
        self.source_name = source["SourceName"]

        if agency != None:
            self.agency = agency
        else:
            self.agency = source["Agency"]

        try:
            self.table_type = defs.TableType(source["TableType"])  # Convert to Enum
        except:
            warnings.warn("{} is not a known table type in defs.TableType".format(source["TableType"]))
            self.table_type = source["TableType"]

        if year_filter != None:
            self.year = year_filter
        else:
            self.year = source["Year"]

        self.description = source["Description"]
        self.url = source["URL"]
        self._data_type = defs.DataType(source["DataType"])  # Convert to Enum

        if not pd.isnull(source["dataset_id"]):
            self._dataset_id = source["dataset_id"]

        if not pd.isnull(source["date_field"]):
            self.date_field = source["date_field"]
        
        if not pd.isnull(source["agency_field"]):
            self.agency_field = source["agency_field"]


    def __repr__(self) -> str:
        skip = ["details", "table"]
        return ',\n'.join("%s: %s" % item for item in vars(self).items() if (item[0] not in skip and item[0][0] != "_"))

    def to_csv(self, output_dir=None, filename=None):
        '''Export table to CSV file. Use default filename for data that will
        be reloaded as an openpolicedata Table object

        Parameters
        ----------
        output_dir - str
            (Optional) Output directory. Default: current directory
        filename - str
            (Optional) Filename. Default: Result of get_csv_filename()
        '''
        if filename == None:
            filename = self.get_csv_filename()
        if output_dir != None:
            filename = path.join(output_dir, filename)
        if not isinstance(self.table, pd.core.frame.DataFrame):
            raise ValueError("There is no table to save to CSV")

        self.table.to_csv(filename, index=False, errors="surrogateescape")

        return filename


    def get_csv_filename(self):
        '''Generate default filename based on table parameters

        Returns
        -------
        str
            Filename
        '''
        return get_csv_filename(self.state, self.source_name, self.agency, self.table_type, self.year)
    

    def get_transform_map(self):
        return self.__transforms
    

    def standardize(self, 
        race_cats="basic",
        agg_race_cat=False,
        eth_cats=None,
        gender_cats=None,
        keep_raw=True,        
        known_cols=None,
        verbose=False,
        no_id="keep",
        race_eth_combo="merge",
        merge_date_time=True,
        empty_time="NaT"
    ):
        if self.__transforms==None:
            if known_cols is None:
                known_cols = {defs.columns.DATE:self.date_field, defs.columns.AGENCY:self.agency_field}

            race_cats = defs.get_race_cats() if race_cats=="basic" else race_cats
            race_cats = defs.get_race_cats(expand=True) if race_cats=="expand" else race_cats
            eth_cats = eth_cats if eth_cats is not None else defs.get_eth_cats()
            gender_cats = gender_cats if gender_cats is not None else defs.get_gender_cats()
            self.table, self.__transforms = preproc.standardize(self.table, self.table_type, self.year,
                known_cols=known_cols, 
                source_name=self.source_name,
                keep_raw=keep_raw,
                verbose=verbose,
                agg_race_cat=agg_race_cat,
                race_cats=race_cats,
                eth_cats=eth_cats,
                gender_cats=gender_cats,
                no_id=no_id,
                race_eth_combo=race_eth_combo,
                merge_date_time=merge_date_time,
                empty_time=empty_time)
        else:
            raise ValueError("Dataset has already been cleaned. Aborting cleaning.")


class Source:
    """
    Class for exploring a data source and loading its data

    ...

    Attributes
    ----------
    datasets : pandas or geopandas DataFrame
        Contains information on datasets available from the source

    Methods
    -------
    get_tables_types()
        Get types of data availble from the source
    get_years(table_type, force)
        Get years available for 1 or more datasets
    get_agencies()
        Get agencies available for 1 or more datasets
    load_from_url()
        Load data from URL
    load_from_csv()
        Load data from a previously saved CSV file
    """

    datasets = None
    __loader = None

    def __init__(self, source_name, state=None):
        '''Constructor for Source class

        Parameters
        ----------
        source_name - str
            Source name from datasets table
        state - str
            (Optional) Name of state. Only necessary if source_name is not unique.

        Returns
        -------
        Source object
        '''
        self.datasets = datasets.query(source_name=source_name, state=state)

        # Ensure that all sources are from the same state
        if len(self.datasets) == 0:
            raise ValueError(f"No Sources Found for {source_name}")
        elif self.datasets["State"].nunique() > 1:
            raise ValueError("Not all sources are from the same state")


    def __repr__(self) -> str:
        return str(self.datasets)


    def get_tables_types(self):
        '''Get types of data availble from the source

        Returns
        -------
        list
            List containing types of data available from source
        '''
        return list(self.datasets["TableType"].unique())


    def get_years(self, table_type, force=False, manual=False):
        '''Get years available for 1 or more datasets

        Parameters
        ----------
        table_type - str or TableType enum
            Only returns years for requested table type
        force - bool
            (Optional) Some data types such as CSV files require reading the whole file to filter for years. By default, an error will be thrown that indicates running load_from_url may be more efficient. For these cases, set force=True to run get_years without error.
        manual - bool
            (Optional) If True, for datasets that contain multiple years, the years will be determined by making requests to the dataset rather than using the years stored in the dataset table. The default is False, which runs faster.

        Returns
        -------
        list
            List of years available for 1 or more datasets
        '''
        dfs = self.__find_datasets(table_type)

        cur_year = datetime.now().year
        all_years = list(dfs["Year"])
        years = set()
        for k in range(len(all_years)):
            if all_years[k] != defs.MULTI:
                years.add(all_years[k])
            else:
                df = dfs.iloc[k]
                _check_version(df)
                data_type =defs.DataType(df["DataType"])
                url = df["URL"]
                date_field = df["date_field"] if pd.notnull(df["date_field"]) else None
                
                loader = self.__get_loader(data_type, url, dataset_id=df["dataset_id"], date_field=date_field)

                check = None
                if not manual and pd.notnull(df["coverage_start"]) and pd.notnull(df["coverage_end"]) and \
                    hasattr(df["coverage_start"], 'year') and hasattr(df["coverage_end"], 'year'):
                    years.update(range(df["coverage_start"].year, df["coverage_end"].year+1))
                    if df["coverage_end"].year >= cur_year-2:
                        # Check for updates
                        check = [x for x in range(df["coverage_end"].year+1,cur_year+1)]
                        if len(check)>0:
                            new_years = loader.get_years(force=force, check=check)
                            years.update(new_years)
                else:
                    new_years = loader.get_years(force=force)
                    years.update(new_years)
            
        years = list(years)
        years.sort()

        return years


    def get_agencies(self, table_type=None, year=None, partial_name=None):
        '''Get agencies available for 1 or more datasets

        Parameters
        ----------
        table_type - str or TableType enum
            (Optional) If set, only returns agencies for requested table type
        year - int or the strings opd.defs.MULTI or opd.defs.NONE
            (Optional)  If set, only returns agencies for requested year
        table_type - str or TableType enum
            (Optional) If set, only returns agencies for requested table type
        partial_name - str
            (Optional)  If set, only returns agencies containing the substring
            partial_name for datasets that contain multiple agencies

        Returns
        -------
        list
            List of agencies available for 1 or more datasets
        '''

        src = self.__find_datasets(table_type)

        if year != None:
            src = src[src["Year"] == year]

        if len(src) == 1:
            src = src.iloc[0]
        else:
            raise ValueError("table_type and year inputs must filter for a single source")            

        # If year is opd.defs.MULTI, need to use self._agencyField to query URL
        # Otherwise return self.agency
        if src["Agency"] == defs.MULTI:
            _check_version(src)
            data_type =defs.DataType(src["DataType"])
            loader = self.__get_loader(data_type, src["URL"], dataset_id=src["dataset_id"], date_field=src["date_field"], agency_field=src["agency_field"])
            if data_type ==defs.DataType.CSV:
                raise NotImplementedError(f"Unable to get agencies for {data_type}")
            elif data_type ==defs.DataType.ArcGIS:
                raise NotImplementedError(f"Unable to get agencies for {data_type}")
            elif data_type ==defs.DataType.SOCRATA:
                if partial_name is not None:
                    opt_filter = src["agency_field"] + " LIKE '%" + partial_name + "%'"
                else:
                    opt_filter = None

                select = "DISTINCT " + src["agency_field"]
                if year == defs.MULTI:
                    year = None

                agency_set = loader.load(year, opt_filter=opt_filter, select=select, output_type="set")
                return list(agency_set)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
        else:
            return [src["Agency"]]


    def get_count(self, year=None, table_type=None, agency=None, force=False):
        '''Get number of records for a data request

        Parameters
        ----------
        year (Optional) - int or length 2 list or the string opd.defs.MULTI or opd.defs.NONE
            Used to identify the requested dataset if equal to its year value
            Otherwise, for datasets containing multiple years, this filters 
            the return data for a specific year (int input) or a range of years
            [X,Y] to return data for years X to Y
        table_type - str or TableType enum
            (Optional) If set, requested dataset will be of this type
        agency - str
            (Optional) If set, for datasets containing multiple agencies, data will
            only be returned for this agency
        force - bool
            (Optional) For file-based data, an exception will be thrown unless force 
            is true. It may be more efficient to load the data and extract the years
            manually

        Returns
        -------
        Table
            Table object containing the requested data
        '''

        return self.__load(year, table_type, agency, True, pbar=False, return_count=True, force=force)
    
    
    def load_from_url_gen(self, year, table_type=None, agency=None, pbar=False, nbatch=10000, offset=0, force=False):
        '''Get generator to load data from URL in batches

        Parameters
        ----------
        year - int or length 2 list or the string opd.defs.MULTI or opd.defs.NONE
            Used to identify the requested dataset if equal to its year value
            Otherwise, for datasets containing multiple years, this filters 
            the return data for a specific year (int input) or a range of years
            [X,Y] to return data for years X to Y
        table_type - str or TableType enum
            (Optional) If set, requested dataset will be of this type
        agency - str
            (Optional) If set, for datasets containing multiple agencies, data will
            only be returned for this agency
        pbar - bool
            (Optional) Whether to show progress bar when loading data. Default False
        nbatch - int
            (Optional) Number of records to load in each batch. Default is 10000.
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 
            to return records starting from the first.
        force - bool
            (Optional) For file-based data, an exception will be thrown unless force 
            is true. It will be more efficient to read the entire dataset all at once

        Returns
        -------
        Table generator
            generates Table objects containing the requested data
        '''

        count = self.get_count(year, table_type, agency, force)
        for k in range(offset, count, nbatch):
            yield self.__load(year, table_type, agency, True, pbar, nrows=min(nbatch, count-k), offset=k)
    
        

    def load_from_url(self, year, table_type=None, agency=None, pbar=True, nrows=None, offset=0):
        '''Load data from URL

        Parameters
        ----------
        year - int or length 2 list or the string opd.defs.MULTI or opd.defs.NONE
            Used to identify the requested dataset if equal to its year value
            Otherwise, for datasets containing multiple years, this filters 
            the return data for a specific year (int input) or a range of years
            [X,Y] to return data for years X to Y
        table_type - str or TableType enum
            (Optional) If set, requested dataset will be of this type
        agency - str
            (Optional) If set, for datasets containing multiple agencies, data will
            only be returned for this agency
        pbar - bool
            (Optional) Whether to show progress bar when loading data. Default True
        nrows - int or None
            (Optional) Number of records to read. Default is None for all records.
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 
            to return records starting from the first.

        Returns
        -------
        Table
            Table object containing the requested data
        '''

        return self.__load(year, table_type, agency, True, pbar, nrows=nrows, offset=offset)

    def __find_datasets(self, table_type):
        if isinstance(table_type, defs.TableType):
            table_type = table_type.value

        src = self.datasets.copy()
        if table_type != None:
            src = src[self.datasets["TableType"].str.upper() == table_type.upper()]

        return src


    def __load(self, year, table_type, agency, load_table, pbar=True, return_count=False, force=False, nrows=None, offset=0):
        
        src = self.__find_datasets(table_type)

        if isinstance(year, list):
            matchingYears = src["Year"] == year[0]
            for y in year[1:]:
                matchingYears = matchingYears | (src["Year"] == y)
        else:
            matchingYears = src["Year"] == year

        filter_by_year = not matchingYears.any()
        if not filter_by_year:
            # Use source for this specific year if available
            src = src[matchingYears]
        else:
            # If there are not any years corresponding to this year, check for a table
            # containing multiple years
            matchingYears = src["Year"]==defs.MULTI
            if matchingYears.any():
                src = src[matchingYears]
            else:
                src = src[src["Year"] == defs.MULTI]

        if isinstance(src, pd.core.frame.DataFrame):
            if len(src) == 0:
                raise ValueError(f"There are no sources matching tableType {table_type} and year {year}")
            elif len(src) > 1:
                raise ValueError(f"There is more than one source matching tableType {table_type} and year {year}")
            else:
                src = src.iloc[0]

        # Load data from URL. For year or agency equal to opd.defs.MULTI, filtering can be done
        data_type =DataType(src["DataType"])
        url = src["URL"]

        if filter_by_year:
            year_filter = year
        else:
            year_filter = None

        if not pd.isnull(src["dataset_id"]):
            dataset_id = src["dataset_id"]
        else:
            dataset_id = None

        table_year = None
        if not pd.isnull(src["date_field"]):
            date_field = src["date_field"]
            if year_filter != None:
                table_year = year_filter
        else:
            date_field = None
        
        table_agency = None
        if not pd.isnull(src["agency_field"]):
            agency_field = src["agency_field"]
            if agency != None and data_type !=defs.DataType.ArcGIS:
                table_agency = agency
        else:
            agency_field = None
        
        #It is assumed that each data loader method will return data with the proper data type so date type etc...
        if load_table:
            _check_version(src)
            loader = self.__get_loader(data_type, url, dataset_id=dataset_id, date_field=date_field, agency_field=agency_field)

            opt_filter = None
            if agency != None and agency_field != None:
                # Double up any apostrophes for SQL query
                agency = agency.replace("'","''")
                opt_filter = agency_field + " = '" + agency + "'"
            
            if return_count:
                return loader.get_count(year=year_filter, agency=agency, opt_filter=opt_filter, force=force)
            else:
                table = loader.load(year=year_filter, agency=agency, opt_filter=opt_filter, nrows=nrows, pbar=pbar, offset=offset)
                table = _check_date(table, date_field)                        
        else:
            table = None

        return Table(src, table, year_filter=table_year, agency=table_agency)


    def load_from_csv(self, year, output_dir=None, table_type=None, agency=None, zip=True):
        '''Load data from previously saved CSV file
        
        Parameters
        ----------
        year - int or length 2 list or the string opd.defs.MULTI or opd.defs.NONE
            Used to identify the requested dataset if equal to its year value
            Otherwise, for datasets containing multiple years, this filters 
            the return data for a specific year (int input) or a range of years
            [X,Y] to return data for years X to Y
        output_dir - str
            (output_dirOptional) Directory where CSV file is stored
        table_type - str or TableType enum
            (Optional) If set, requested dataset will be of this type
        agency - str
            (Optional) If set, for datasets containing multiple agencies, data will
            only be returned for this agency
        zip - bool
            (Optional) Set to true if CSV is in a zip file with the same filename. Default: False

        Returns
        -------
        Table
            Table object containing the requested data
        '''

        table = self.__load(year, table_type, agency, False)

        filename = table.get_csv_filename()
        if output_dir != None:
            filename = path.join(output_dir, filename)   

        if zip:
            filename = filename.replace(".csv",'.zip')         

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r"Columns \(.+\) have mixed types", category=pd.errors.DtypeWarning)
            table.table = pd.read_csv(filename, parse_dates=True, encoding_errors='surrogateescape')
        table.table = _check_date(table.table, table.date_field)  

        return table

    def get_csv_filename(self, year, output_dir=None, table_type=None, agency=None):
        '''Get auto-generated CSV filename
        
        Parameters
        ----------
        year - int or length 2 list or the string opd.defs.MULTI or opd.defs.NONE
            Used to identify the requested dataset if equal to its year value
            Otherwise, for datasets containing multiple years, this filters 
            the return data for a specific year (int input) or a range of years
            [X,Y] to return data for years X to Y
        output_dir - str
            (Optional) Directory where CSV file is stored
        table_type - str or TableType enum
            (Optional) If set, requested dataset will be of this type
        agency - str
            (Optional) If set, for datasets containing multiple agencies, data will
            only be returned for this agency

        Returns
        -------
        str
            Auto-generated CSV filename
        '''

        table = self.__load(year, table_type, agency, False)

        filename = table.get_csv_filename()
        if output_dir != None:
            filename = path.join(output_dir, filename)             

        return filename

    def __get_loader(self, data_type, url, dataset_id=None, date_field=None, agency_field=None):
        if pd.isnull(dataset_id):
            dataset_id = None
        params = (data_type, url, dataset_id, date_field, agency_field)
        if self.__loader is not None and self.__loader[0]==params:
            return self.__loader[1]

        if data_type ==defs.DataType.CSV:
            loader = data_loaders.Csv(url, date_field=date_field, agency_field=agency_field)
        elif data_type ==defs.DataType.EXCEL:
            loader = data_loaders.Excel(url, date_field=date_field, agency_field=agency_field) 
        elif data_type ==defs.DataType.ArcGIS:
            loader = data_loaders.Arcgis(url, date_field=date_field)
        elif data_type ==defs.DataType.SOCRATA:
            loader = data_loaders.Socrata(url, dataset_id, date_field=date_field)
        elif data_type ==defs.DataType.CARTO:
            loader = data_loaders.Carto(url, dataset_id, date_field=date_field)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

        self.__loader = (params, loader)

        return loader

def _check_date(table, date_field):
    if date_field != None and table is not None and len(table)>0:
        dts = table[date_field]
        dts = dts[dts.notnull()]
        if len(dts) > 0:
            one_date = dts.iloc[0]  
            if type(one_date) == pd._libs.tslibs.timestamps.Timestamp:
                table[date_field] = pd.to_datetime(table[date_field], errors='ignore')
            elif type(one_date) == str:
                p = re.compile(r'^Unknown string format: \d{4}-(\d{2}|__)-(\d{2}|__) present at position \d+$')
                def to_datetime_local(x):
                    try:
                        return to_datetime(x, errors='ignore')
                    except ParserError as e:
                        if len(e.args)>0 and p.match(e.args[0]) != None:
                            return pd.NaT
                        else:
                            raise
                    except:
                        raise
                
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="Could not infer format", category=UserWarning)
                    table[date_field] = table[date_field].apply(to_datetime_local)
                # table = table.astype({date_field: 'datetime64[ns]'})
            elif ("year" in date_field.lower() or date_field.lower() == "yr") and isinstance(one_date, numbers.Number):
                table[date_field] = table[date_field].apply(lambda x: datetime(x,1,1))
                
            # Replace bad dates with NaT
            table[date_field].replace(datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'), pd.NaT, inplace=True)


    return table


def get_csv_filename(state, source_name, agency, table_type, year):
    '''Get default CSV filename for the given parameters. Enables reloading of data from CSV.
    
    Parameters
    ----------
    state - str
        Name of state
    source_name - str
        Name of source
    agency - str
        Name of agency
    table_type - str or TableType enum
        Type of data
    year = int or length 2 list or the string opd.defs.MULTI or opd.defs.NONE
        Year of data to load, range of years of data to load as a list [X,Y]
        to load years X to Y, or a string to indicate all of multiple year data
        (opd.defs.MULTI) or a dataset that has no year filtering ("N/A")

    Returns
    -------
    str
        Default CSV filename
    '''
    if isinstance(table_type, defs.TableType):
        table_type = table_type.value
        
    filename = f"{state}_{source_name}"
    if source_name != agency:
        filename += f"_{agency}"
    filename += f"_{table_type}"
    if isinstance(year, list):
        filename += f"_{year[0]}_{year[-1]}"
    else:
        filename += f"_{year}"

    # Clean up filename
    filename = filename.replace(",", "_").replace(" ", "_").replace("__", "_").replace("/", "_")

    filename += ".csv"

    return filename

def _check_version(df):
    min_version = df["min_version"] 
    if pd.notnull(min_version):
        src_name = df["SourceName"]
        state = df["State"]
        table_type = df["TableType"]
        year = df["Year"]
        if min_version == "-1":
            raise exceptions.OPD_FutureError(
                f"Year {year} {table_type} data for {src_name} in {state} cannot be loaded in this version. " + \
                    "It will be made available in a future release"
            )
        elif version.parse(__version__) < version.parse(min_version):
            raise exceptions.OPD_MinVersionError(
                f"Year {year} {table_type} data for {src_name} in {state} cannot be loaded in version {__version__} of openpolicedata. " + \
                    f"Update OpenPoliceData to at least version {min_version} to access this data."
            )

