from abc import ABC, abstractmethod
from datetime import date, datetime
from io import BytesIO
import numbers
import json
import pandas as pd
import re
import requests
from time import sleep
from tqdm import tqdm
import urllib
import urllib3
import warnings
from zipfile import ZipFile

from ..datetime_parser import to_datetime
from ..exceptions import DateFilterException
from .. import log, httpio
from ..utils import is_str_number

try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

logger = log.get_logger()

# Global parameter for testing both with and without GeoPandas in testing
_use_gpd_force = None

# Default number of records to read per request
_default_limit = 100000

_url_error_msg = "There is likely an issue with the website. Open the URL {} with a web browser to confirm. " + \
                    "See a list of known site outages at https://github.com/openpolicedata/opd-data/blob/main/outages.csv"
def _check_year(year):
    return isinstance(year, int) or (isinstance(year, str) and len(year)==4 and year.isdigit())


def _process_date(date, date_field=None, force_year=False, datetime_format=None, is_date_string=False):
    if not isinstance(date, list):
        date = [date, date]

    if len(date)!=2:
        raise ValueError("date should be a list of length 2: [startYear, stopYear]")
    
    is_year = force_year or (date_field != None and 'year' in date_field.lower())
    if is_year:
        for d in date:
            if not _check_year(d):
                raise DateFilterException(f"Column {date_field} is not a date column. It either contains a year or is a date but in a text format. "+
                                "Currently, only a year filter is allowed for this case, but the input {d} appears to not be a year.")
        start_date = str(date[0])
        stop_date = str(date[1])
    else:
        if isinstance(date[0], str) and re.search(r'^\d{4}-\d{2}-\d{2}$', date[0]):  # YYYY-MM-DD
            start_date = date[0]
        elif isinstance(date[0], numbers.Number) or re.search(r'^\d{4}$', date[0]):   # YYYY
            start_date = str(date[0]) + "-01-01"
        else:
            raise ValueError(f"Unknown date input {date[0]}")

        if isinstance(date[1], str) and re.search(r'^\d{4}-\d{2}-\d{2}$', date[1]):  # YYYY-MM-DD
            # If date date are strings, the ASCII character for z will be greater than all possible values that follow date[1]
            stop_date  = str(date[1])+'zz' if is_date_string else str(date[1])+"T23:59:59.999"
        elif isinstance(date[1], numbers.Number) or re.search(r'^\d{4}$', date[1]):   # YYYY
            # If date date are strings, the ASCII character for z will be greater than all possible values that follow date[1]
            stop_date  = str(date[1])+'-12-31zz' if is_date_string else str(date[1])+"-12-31T23:59:59.999"
        else:
            raise ValueError(f"Unknown date input {date[1]}")

    if datetime_format:
        start_date = datetime.strftime(pd.to_datetime(start_date), datetime_format)
        stop_date = datetime.strftime(pd.to_datetime(stop_date), datetime_format)

    if start_date > stop_date:
        raise ValueError(f'Start date {start_date} needs to be less than or equal to stop date {stop_date}')

    return start_date, stop_date


def filter_dataframe(df, date_field=None, year_filter=None, agency_field=None, agency=None, format_date=True):
    '''Filter dataframe by agency and/or year (range)
    
    Parameters
    ----------
    df : pandas or geopandas dataframe
        Dataframe containing the data
    date_field : str
        (Optional) Name of the column that contains the date
    year_filter : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
    agency_field : str
        (Optional) Name of the column that contains the agency name (i.e. name of the police departments)
    agency : str
        (Optional) Name of the agency to filter for. None value returns data for all agencies.
    format_date : bool, optional
        If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
        to be pandas datetimes (or pandas Period in rare cases), by default True
    '''

    if agency != None and agency_field != None:
        logger.debug(f"Keeping values of column {agency_field} that are equal to {agency}")
        df = df.query(agency_field + " = '" + agency + "'")

    if pd.notnull(date_field):
        is_year = date_field.lower()=='year'
        if not is_year and pd.api.types.is_integer_dtype(df[date_field]):
            is_year = ((df[date_field] >= 1900) & (df[date_field] <= 2200)).all()

        if not is_year and not hasattr(df[date_field], "dt"):
            with warnings.catch_warnings():
                # Ignore future warning about how this operation will be attempted to be done inplace:
                # In a future version, `df.iloc[:, i] = newvals` will attempt to set the values inplace instead of always setting a new array. 
                # To retain the old behavior, use either `df[df.columns[i]] = newvals` or, if columns are non-unique, `df.isetitem(i, newvals)`
                logger.debug(f"Converting values in column {date_field} to datetime objects")
                try:
                    df[date_field] = to_datetime(df[date_field], ignore_errors=True)
                except:
                    return df
    
        if year_filter != None:
            if not format_date:
                raise ValueError("Dates cannot be filtered if format_date is False for CSV and Excel data types")
            if isinstance(year_filter, list):
                if len(year_filter) != 2:
                    raise ValueError(f'Format of the input {year_filter} is invalid.'
                                     'Date/year filters that are lists are expected to be a length 2 list of ' +
                                     '[startYear, stopYear] or [startDate, stopDate]. Dates should be in '+
                                     'YYYY-MM-DD format.')
                if not is_year:
                    if isinstance(year_filter[0],int) or is_str_number(year_filter[0]):
                        year_filter[0] = f"{year_filter[0]}-01-01"
                    elif not (re.search(r'\d{4}-\d{2}-\d{2}', year_filter[0]) or \
                              re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', year_filter[0])):
                        raise ValueError(f"{year_filter[0]} must be in YYYY-MM-DD format")
                    
                    if isinstance(year_filter[-1],int) or is_str_number(year_filter[0]):
                        year_filter[-1] = f"{year_filter[-1]}-12-31T23:59:59.999"
                    elif re.search(r'\d{4}-\d{2}-\d{2}', year_filter[1]):
                        year_filter[-1] = f"{year_filter[-1]}T23:59:59.999"
                    elif not re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', year_filter[1]):
                        raise ValueError(f"{year_filter[0]} must be in YYYY-MM-DD format")
                    logger.debug(f"Keeping values of column {date_field} between {year_filter[0]} and {year_filter[1]}")
                    df = df[(df[date_field] >= year_filter[0]) & (df[date_field] <= year_filter[1])]
                elif (isinstance(year_filter[0],int) or is_str_number(year_filter[0])) and \
                     (isinstance(year_filter[1],int) or is_str_number(year_filter[1])):
                    logger.debug(f"Column {date_field} has been identfied as a year column")
                    logger.debug(f"Keeping values of column {date_field} between {year_filter[0]} and {year_filter[1]}")
                    df = df[df[date_field].isin(range(year_filter[0], year_filter[1]+1))]
                else:
                    raise ValueError(f"Column {date_field} has been identfied as a year column and cannot be filtered by dates: {year_filter}")
            elif not is_year:
                logger.debug(f"Keeping values of column {date_field} for year={year_filter}")
                df = df[df[date_field].dt.year == int(year_filter)]
            else:
                logger.debug(f"Column {date_field} has been identfied as a year column")
                logger.debug(f"Keeping values of column {date_field} for year={year_filter}")
                df = df[df[date_field] == int(year_filter)]

    return df

def get_legacy_session():
    try:
        import ssl
    except:
        raise ImportError(f"Loading this dataset requires the SSL package, which typically comes with the Python installation" + 
                          " but is not for some Python versions like the one used by Jupyter Lite. To install, run 'pip install ssl'")
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session


# Based on https://stackoverflow.com/a/73519818/9922439
class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)
        

class UrlIoContextManager:
    def __init__(self, url) -> None:
        self.url = url
        try:
            self.file = httpio.open(url)
            self.ishttp = True
        except httpio.HTTPIOError:
            open_url =  urllib.request.urlopen(url)
            self.file = BytesIO(open_url.read())
            self.ishttp = False

    def __enter__(self):
        return self.file
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.ishttp:
            self.file.close()
        

def download_zip_and_extract(url, block_size, pbar=True):
    r = requests.get(url, stream=True)
    r.raise_for_status()
    total_size = int(r.headers.get("Content-Length", 0))
    pbar = pbar and total_size > block_size
    if pbar:
        bar = tqdm(
            desc=f"Downloading zip file: {url}",
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            leave=False
        )
    b = BytesIO()
    for data in r.iter_content(block_size):
        b.write(data)
        if pbar:
            bar.update(len(data))
    r.close()

    logger.debug(f'Completed downloading CSV zip file: {url}')
    if pbar:
        bar.close()
    b.seek(0)

    logger.debug('Creating zip file')
    with ZipFile(b, 'r') as z:
        if len(z.namelist())>1:
            raise ValueError(f"More than 1 file found in {url} but no file was specified by the user. Please specify 1 or more files in the dataset input.")

        logger.debug('Reading from zip file')
        zip_data = z.read(z.namelist()[0])
        b.close()
        logger.debug('Converting to BytesIO')

    return zip_data

def str2json(json_str):
    if pd.isnull(json_str):
        return {}
    elif isinstance(json_str, dict):
        return json_str
    # Remove any curly quotes
    json_str = json_str.replace('“','"').replace('”','"')
    return json.loads(json_str)

sleep_time = 0.1

class Data_Loader(ABC):
    """Base class for data loaders

    Methods
    -------
    load(year=None, nrows=None, pbar=True, agency=None, opt_filter=None, select=None, output_type=None)
        Load data for query
    get_count(year=None, agency=None, force=False, opt_filter=None, where=None)
        Get number of records/rows generated by query
    get_years(nrows=1)
        Get years contained in data set
    """

    _last_count = None

    @abstractmethod
    def isfile(self):
        pass

    @abstractmethod
    def get_count(self, year=None, *, agency=None, force=False, opt_filter=None, where=None):
        pass

    @abstractmethod
    def load(self, year=None, nrows=None, offset=0, *, pbar=True, agency=None, opt_filter=None, select=None, output_type=None, format_date=True):
        pass

    def get_years(self, *, nrows=1, check=None, **kwargs):
        '''Get years contained in data set
        
        Parameters
        ----------
        nrows : int
            (Optional) Number of records to load when checking each year
            
        Returns
        -------
        list
            list containing years in data set
        '''

        if self.date_field==None:
            raise ValueError("A date field is required to get years")

        check_input = check is not None

        if check_input and len(check)==0:
            return []

        if check_input:
            check = check.copy()
            check.sort(reverse=True)
            year = check.pop(0)
        else:
            year = date.today().year

        oldest_recent = 20
        max_misses_gap = 10
        max_misses = oldest_recent
        misses = 0
        years = []
        while misses < max_misses:
            count = self.get_count(year=year)

            if count==0:  # If doesn't have len attribute, it is None
                misses+=1
            else:
                misses = 0
                max_misses = max_misses_gap
                years.append(year)

            sleep(sleep_time)

            year-=1
            if check_input:
                if len(check)==0:
                    break
                while year not in check:
                    year-=1
                check.remove(year)

        return years
 