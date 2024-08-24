from dataclasses import dataclass
from io import BytesIO
import itertools
import json
import logging
import numbers
import os
import tempfile
from datetime import date, datetime
import pandas as pd
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from numpy import nan
import requests
import urllib
import urllib3
from abc import ABC, abstractmethod
from sodapy import Socrata as SocrataClient
import warnings
from time import sleep
from tqdm import tqdm
from typing import Optional
from math import ceil
import re
from xlrd.biffh import XLRDError
from zipfile import ZipFile

try:
    import geopandas as gpd
    from shapely.geometry import Point
    _has_gpd = True
except:
    _has_gpd = False

try:
    from .datetime_parser import to_datetime
    from .exceptions import OPD_TooManyRequestsError, OPD_DataUnavailableError, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, DateFilterException
    from .utils import is_str_number
except:
    from datetime_parser import to_datetime
    from exceptions import OPD_TooManyRequestsError, OPD_DataUnavailableError, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, DateFilterException

logger = logging.getLogger("opd-load")

sleep_time = 0.1

# Global parameter for testing both with and without GeoPandas in testing
_use_gpd_force = None

# Default number of records to read per request
_default_limit = 100000
_url_error_msg = "There is likely an issue with the website. Open the URL {} with a web browser to confirm. " + \
                    "See a list of known site outages at https://github.com/openpolicedata/opd-data/blob/main/outages.csv"

# Flag to indicate if ArcGIS queries should be verified against the arcgis package. Used in testing
_verify_arcgis = False


# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: http://dev.socrata.com/docs/app-tokens.html
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
# Windows: https://www.wikihow.com/Create-an-Environment-Variable-in-Windows-10
default_sodapy_key = os.environ.get("SODAPY_API_KEY")

class repeat_format(object):
    def __init__(self, string):
        self.string = string
        self.repeat = int(string.count('{}')/2)

    def __eq__(self, other) -> bool:
        return isinstance(other, repeat_format) and self.string == other.string

    def format(self, date_field, year):
        args = []
        for _ in range(self.repeat):
            args.extend([date_field, year])
        args = tuple(args)
        return self.string.format(*args)
    
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

def read_zipped_csv(url, pbar=True, block_size=2**20):
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
    if pbar:
        bar.close()
    b.seek(0)
    z = ZipFile(b, 'r')
    return pd.read_csv(BytesIO(z.read(z.namelist()[0])), encoding_errors='surrogateescape')


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
    

class CombinedDataset(Data_Loader):
    """
    A class for combining multiple datasets of a single type (Csv, Excel, etc.) into a single data loader

    Parameters
    ----------
    data_class: abc.ABCMeta 
        Data loader class of datasets to be combined
    loaders: list[Data_Loader]
        Individual data loader objects

    Methods
    -------
    CombinedDataset(data_class, url, datasets, *args, **kwargs)
        Constructor.
    load(*args, **kwargs)
        Load data for query. 
    get_count(*args, **kwargs)
        Get number of records/rows generated by query
    get_years(*args, **kwargs)
        Get years contained in data set
    """

    def __init__(self, data_class, url, datasets, *args, **kwargs):
        """CombinedDataset constructor

        Parameters
        ----------
        data_class : abc.ABCMeta 
            Class of data loader object
        url : str
            Base URL of dataset (will be combined with each value in datasets)
        datasets : str or List[str]
            Relative dataset URLs (each value will be combined with url). Can be a semi-colon separate string or a list.

        *args and **kwargs will be passed to the constructor of data_class
        """
        self.data_class = data_class
        sheets = None
        if isinstance(datasets, str):
            if '|' in datasets:  # dataset names are separated from relative URLs by |
                datasets = datasets.split('|')
                assert len(datasets)==2
                sheets = datasets[0].split(';')   # Different sheet names for each dataset are separated by ;. If multiple sheets for a given dataset, separate by &
                datasets = datasets[1]
            datasets = datasets.split(';')         # Multiple relative URLs are separated by ;
            
        self.loaders = []
        url = url[:-1] if url[-1]=='/' else url
        for k, ds in enumerate(datasets):
            kwargs = {}
            if 'raw.githubusercontent.com/openpolicedata/opd-datasets' in ds and ds.endswith('.csv'):
                self.loaders.append(Csv(ds, *args, **kwargs))
            else:
                ds = ds.strip()
                ds = ds[1:] if ds[0]=='/' else ds
                ds = url + '/' + ds
                if sheets:
                    kwargs['data_set'] = sheets[min(k, len(sheets)-1)]
                try:
                    self.loaders.append(data_class(ds, *args, **kwargs))
                except ValueError as e:
                    if str(e)=='Excel file format cannot be determined, you must specify an engine manually.':
                        try:
                            # This may be a CSV file instead of an Excel file
                            self.loaders.append(Csv(ds, *args, **kwargs))
                        except:
                            raise e
                        
                if ds!=datasets[-1]:
                    sleep(0.5)  # Reduce likelihood of timeout due to repeated requests



    def isfile(self):
        '''Returns True to indicate that Csv data is file-based

        Returns
        -------
        True
        '''
        return True
    
    def load(self, **kwargs):
        """Load data for query. 

        **kwargs will be passed to the load function of the data_class
        """

        # Handle these here. Less efficient but easier to implement
        nrows = kwargs.pop('nrows') if 'nrows' in kwargs else None
        offset = kwargs.pop('offset') if 'offset' in kwargs else None

        dfs = []
        date_warn = force_subject_warn = force_officer_warn = False
        for loader in self.loaders:
            dfs.append(loader.load(**kwargs))
            if loader!=self.loaders[-1]:
                sleep(0.5)  # Reduce likelihood of timeout due to repeated requests

            if 'www.albemarle.org' in loader.url:
                # Renamed
                if 'Stop Date' in dfs[-1] and 'Date' in dfs[0]: # Column renamed
                    dfs[-1] = dfs[-1].rename(columns={'Stop Date':'Date'})
                    if not date_warn:
                        date_warn = True
                        warnings.warn("Renaming date column because name of column names changes in some of the monthly data files")
                if 'Force Used by Subject' in dfs[-1] and 'Physical Force by Subject' in dfs[0]: # Column renamed
                    dfs[-1] = dfs[-1].rename(columns={'Force Used by Subject':'Physical Force by Subject'})
                    if not force_subject_warn:
                        force_subject_warn = True
                        warnings.warn("Renaming force by subject column because name of column names changes in some of the monthly data files")
                if 'Force Used by Officer' in dfs[-1] and 'Physical Force by Officer' in dfs[0]: # Column renamed
                    dfs[-1] = dfs[-1].rename(columns={'Force Used by Officer':'Physical Force by Officer'})
                    if not force_officer_warn:
                        force_officer_warn = True
                        warnings.warn("Renaming force by officer column because name of column names changes in some of the monthly data files")

        df = pd.concat(dfs, ignore_index=True)
        if offset!=None:
            df = df.iloc[offset:]
        if nrows!=None:
            df = df.head(nrows)

        return df
        
    def get_count(self, *args, **kwargs):   
        """Get number of records/rows generated by query

        *args and **kwargs will be passed to the load function of the data_class
        """

        count = 0
        for loader in self.loaders:
            count+=loader.get_count(*args, **kwargs)

        return count
    
    
    def get_years(self, *args, **kwargs):   
        """Get years contained in data set

        *args and **kwargs will be passed to the load function of the data_class
        """

        years = []
        for loader in self.loaders:
            years.extend(loader.get_years(*args, **kwargs))

        return years


class Csv(Data_Loader):
    """
    A class for accessing data from CSV download URLs

    Parameters
    ----------
    url : str
        URL
    date_field : str
        Name of the column that contains the date
    agency_field : str
        Name of column that contains agency names

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True, agency=None)
        Load data for query
    get_count(year=None, agency=None, force=False)
        Get number of records/rows generated by query
    get_years(force=False)
        Get years contained in data set
    """

    def __init__(self, url, date_field=None, agency_field=None):
        '''Create Csv object

        Parameters
        ----------
        url : str
            URL for CSV data
        date_field : str
            (Optional) Name of the column that contains the date
        agency_field : str
                (Optional) Name of the column that contains the agency name (i.e. name of the police departments)
        '''
        
        self.url = url
        self.date_field = date_field
        self.agency_field = agency_field


    def isfile(self):
        '''Returns True to indicate that Csv data is file-based

        Returns
        -------
        True
        '''
        return True


    def get_count(self, year=None, *,  agency=None, force=False, **kwargs):
        '''Get number of records for a Csv data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        agency : str
            (Optional) Name of agency to filter for.
        force : bool
            (Optional) get_count for CSV file will only run if force=true. In many use cases, it will be more efficient to load the file and manually get the count.
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''

        logger.debug(f"Calculating row count for {self.url}")
        if self._last_count is not None and self._last_count[0] == (self.url, year, agency):
            logger.debug("Request matches previous count request. Returning saved count.")
            return self._last_count[1]
        if ".zip" not in self.url and year==None and agency==None:
            count = 0
            logger.debug(f"Loading file from {self.url}")
            with requests.get(self.url, stream=True) as r:
                for chunk in r.iter_content(chunk_size=2**16):
                    count += chunk.count(b"\n")

            # Subtract off trailing newlines in last row
            newline = int.from_bytes(b"\n", "big")
            for c in reversed(chunk):
                if c==newline:
                    count-=1
                else:
                    break
        elif force:
            count = len(self.load(year=year, agency=agency))
        else:
            raise ValueError("Extracting the number of records for a single year of a CSV file requires reading the whole file in. In most cases, "+
                "running load() with a year argument to load in the data and manually finding the record count will be more "
                "efficient. If running get_count with a year argument is still desired, set force=True")
        
        self._last_count = ((self.url, year, agency), count)
        return count


    def load(self, year=None, nrows=None, offset=0, *, pbar=True, agency=None, format_date=True, **kwargs):
        '''Download CSV file to pandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested. None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the CSV
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        pbar : bool
            (Optional) If true (default), a progress bar will be displayed
        agency : str
            (Optional) Name of the agency to filter for. None value returns data for all agencies.
        format_date : bool, optional
            If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
            to be pandas datetimes (or pandas Period in rare cases), by default True
            
        Returns
        -------
        pandas DataFrame
            DataFrame containing table imported from CSV
        '''

        if isinstance(nrows, float):
            nrows = int(nrows)
        
        logger.debug(f"Loading file from {self.url}")
        if ".zip" in self.url:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=pd.errors.DtypeWarning)
                try:
                    table = read_zipped_csv(self.url, pbar=pbar)
                except requests.exceptions.HTTPError as e:
                    if len(e.args) and 'Forbidden' in e.args[0]:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            # 'Accept-Encoding': 'gzip, deflate, br',
                            'DNT': '1',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Sec-Fetch-User': '?1',
                        }
                        try:
                            table = pd.read_csv(self.url, encoding_errors='surrogateescape', storage_options=headers)
                        except urllib.error.HTTPError as e:
                            raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
                        except:
                            raise
                    else:
                        raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
                except Exception as e:
                    raise e
        else:
            use_legacy = False
            headers = None
            try:
                r = requests.head(self.url)
            except requests.exceptions.SSLError as e:
                if "[SSL: UNSAFE_LEGACY_RENEGOTIATION_DISABLED] unsafe legacy renegotiation disabled" in str(e.args[0]) or \
                    "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate" in str(e.args[0]):
                    use_legacy = True
                elif 'Max retries exceeded' in str(e):
                    raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
                else:
                    raise e
            except requests.exceptions.ConnectionError as e:
                if 'Max retries exceeded' in str(e):
                    raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
                else:
                    raise e
            except Exception as e:
                raise
                
            if not use_legacy:
                if r.status_code in [400,404]:
                    # Try get instead
                    r = requests.get(self.url)
                try:
                    r.raise_for_status()
                    r.close()
                except requests.exceptions.HTTPError as e:
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            # 'Accept-Encoding': 'gzip, deflate, br',
                            'DNT': '1',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Sec-Fetch-User': '?1',
                        }
                        r = requests.get(self.url, headers=headers)
                        r.raise_for_status()
                        r.close()
                    except:
                        raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
                except Exception as e:
                    raise e
            
            def get(url, use_legacy, headers=None):
                if use_legacy:
                    return get_legacy_session().get(url, params=None, stream=True, headers=headers)
                else:
                    return requests.get(url, params=None, stream=True, headers=headers)

            header = 'infer'   

            with get(self.url, use_legacy, headers) as resp:
                try:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", message=r"Columns \(.+\) have mixed types", category=pd.errors.DtypeWarning)
                        table = pd.read_csv(TqdmReader(resp, pbar=pbar), nrows=offset+nrows if nrows is not None else None, 
                            encoding_errors='surrogateescape', 
                            header=header)
                except (urllib.error.HTTPError, pd.errors.ParserError) as e:
                    raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
                except Exception as e:
                    raise e
                
        if len(table.columns)==1 and ('?xml' in table.columns[0] or re.search(r'^\<.+\>', table.columns[0])):
            # Read data was not a CSV file. It was an error code or HTML
            raise OPD_DataUnavailableError(table.iloc[0,0], _url_error_msg.format(self.url))
        
        table = filter_dataframe(table, date_field=self.date_field, year_filter=year, 
            agency_field=self.agency_field, agency=agency, format_date=format_date)

        if offset>0:
            rows_limit = offset+nrows if nrows is not None and offset+nrows<len(table) else len(table)
            logger.debug(f"Extracting {rows_limit} rows starting at {offset}")
            table = table.iloc[offset:rows_limit].reset_index(drop=True)
        if nrows is not None and len(table)>nrows:
            logger.debug(f"Extracting the first {nrows} rows")
            table = table.head(nrows)

        return table

    def get_years(self, *, force=False, **kwargs):
        '''Get years contained in data set
        
        Parameters
        ----------
        force : bool
            (Optional) If false, an exception will be thrown. It may be more efficient to load the table and extract years manually
            
        Returns
        -------
        list
            list containing years in data set
        '''

        if not force:
            raise ValueError("Extracting the years of a CSV file requires reading the whole file in. In most cases, "+
                "running load() with no arguments to load in the whole CSV file and manually finding the years will be more "
                "efficient. If running get_years is still desired, set force=True")
        else:
            if self.date_field==None:
                raise ValueError("No date field provided to access year information")
            df = self.load()
            if self.date_field.lower()=="year":
                years = df[self.date_field].unique()
            else:
                date_col = to_datetime(df[self.date_field])
                years = list(date_col.dt.year.dropna().unique())
            years.sort()
            return [int(x) for x in years]


class Excel(Data_Loader):
    """
    A class for accessing data from Excel download URLs

    Parameters
    ----------
    url : str
        URL
    date_field : str
        Name of the column that contains the date
    agency_field : str
        Name of column that contains agency names
    excel_file : Pandas ExcelFile
        Object for use in reading data

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True, agency=None)
        Load data for query
    get_count(year=None, agency=None, force=False)
        Get number of records/rows generated by query
    get_years(force=False)
        Get years contained in data set
    """

    def __init__(self, url, data_set=None, date_field=None, agency_field=None):
        '''Create Excel object

        Parameters
        ----------
        url : str
            URL for Excel data
        date_field : str
            (Optional) Name of the column that contains the date
        agency_field : str
            (Optional) Name of the column that contains the agency name (i.e. name of the police departments)
        data_set : str
            (Optional) Excel sheet to use. If not provided, an error will be thrown when loading data if there is more than 1 sheet
        '''
        
        self.url = url
        self.date_field = date_field
        self.agency_field = agency_field
        self.sheet = data_set

        if self.sheet is not None and re.match(r'^[“”"].+[“”"]$', self.sheet):
            # Sheet name was put in quotes due to it being a number to prevent Excel from dropping any zeros from the front
            self.sheet = self.sheet[1:-1]
        
        try:
            if ".zip" in self.url:
                # Download file to temporary file
                r = requests.get(url)
                r.raise_for_status()
                with tempfile.TemporaryFile(suffix=".zip") as fp:
                    fp.write(r.content)
                    fp.seek(0)

                    z = ZipFile(fp, 'r')
                    self.excel_file = pd.ExcelFile(BytesIO(z.read(z.namelist()[0])))
            else:
                self.excel_file = pd.ExcelFile(url)
        except urllib.error.HTTPError as e:
            if str(e) in ["HTTP Error 406: Not Acceptable", 'HTTP Error 403: Forbidden']:
                # 406 error: https://stackoverflow.com/questions/34832970/http-error-406-not-acceptable-python-urllib2
                # File-like input for URL: https://stackoverflow.com/questions/57815780/how-can-i-directly-handle-excel-file-link-python/57815864#57815864
                headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}
                headers2 = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    # 'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                }
                for k, h in enumerate([headers, headers2]):
                    r = requests.get(url, stream=True, headers=h)
                    try:
                        r.raise_for_status()
                        break
                    except:
                        if k==1:
                            raise
                file_like = BytesIO(r.content)
                self.excel_file = pd.ExcelFile(file_like)
            else:
                raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(self.url))
        except urllib.error.URLError as e:
            if "[SSL: UNSAFE_LEGACY_RENEGOTIATION_DISABLED] unsafe legacy renegotiation disabled" in str(e.args[0]):
                with get_legacy_session() as session:
                    r = session.get(url)
                    
                r.raise_for_status()
                file_like = BytesIO(r.content)
                self.excel_file = pd.ExcelFile(file_like)
            else:
                raise e
        except XLRDError as e:
            if len(e.args)>0 and e.args[0] == "Workbook is encrypted" and \
                any([url.startswith(x) for x in ["http://www.rutlandcitypolice.com"]]):  # Only perform on known datasets to prevent security issues
                try:
                    import msoffcrypto
                except:
                    raise ImportError(f"{url} is encrypted. OpenPoliceData may be able to open it if msoffcrypto-tool " + 
                        "(https://pypi.org/project/msoffcrypto-tool/) is installed (pip install msoffcrypto-tool)")
                # Download file to temporary file
                r = requests.get(url)
                r.raise_for_status()
                # https://stackoverflow.com/questions/22789951/xlrd-error-workbook-is-encrypted-python-3-2-3
                fp_decrypt = tempfile.TemporaryFile(suffix=".xls")
                with tempfile.TemporaryFile() as fp:
                    fp.write(r.content)
                    fp.seek(0)

                    # Try and unencrypt workbook with magic password
                    wb_msoffcrypto_file = msoffcrypto.OfficeFile(fp)

                    # https://nakedsecurity.sophos.com/2013/04/11/password-excel-velvet-sweatshop/
                    wb_msoffcrypto_file.load_key(password='VelvetSweatshop')
                    wb_msoffcrypto_file.decrypt(fp_decrypt)

                fp_decrypt.seek(0)
                self.excel_file = pd.ExcelFile(fp_decrypt)
            else:
                raise
        except Exception as e:
            raise e


    def isfile(self):
        '''Returns True to indicate that Excel data is file-based

        Returns
        -------
        True
        '''
        return True


    def get_count(self, year=None, *,  agency=None, force=False, **kwargs):
        '''Get number of records for a Excel data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        agency : str
            (Optional) Name of agency to filter for.
        force : bool
            (Optional) get_count for Excel file will only run if force=true. In many use cases, it will be more efficient to load the file and manually get the count.
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''

        logger.debug(f"Calculating row count for {self.url}")
        if self._last_count is not None and self._last_count[0]==(self.url, year, agency):
            logger.debug("Request matches previous count request. Returning saved count.")
            return self._last_count[1]
        elif force:
            count = len(self.load(year=year, agency=agency))
            self._last_count = ((self.url, year, agency), count)
            return count
        else:
            raise ValueError("Extracting the number of records for an Excel file requires reading the whole file in. In most cases, "+
                "running load() to load in the data and manually finding the record count will be more "
                "efficient. If running get_count with a year argument is still desired, set force=True")


    def __get_sheets(self):
        names = self.excel_file.sheet_names
        if sum([x.isdigit() for x in names]) / len(names) > 0.75 and len(names)>1:
            logger.debug("Different years of data may be stored in separate Excel sheets. Evaluating...")
            possible_years = [int(x) for x in names if x.isdigit()]
            year_names = [x for x in names if x.isdigit()]

            if self.url=='https://www.arcgis.com/sharing/rest/content/items/73672aa470da4095a88fcac074ee00e6/data':
                # This is the Louisville OIS dataset. The sheets for years before 2011 are empty
                year_names = [x for x,y in zip(year_names, possible_years) if y>2010]
                possible_years = [x for x in possible_years if x>2010]
            
            min_year = min(possible_years)

            if min_year < 2000 or min_year > date.today().year:
                raise ValueError(f"Sheet name {min_year} is not recognized as a year in {self.url}")

            year_dict = {}
            years_found = True
            for i in range(len(possible_years)):
                year = min_year + i
                k = [k for k,x in enumerate(possible_years) if x==year]
                if len(k)==1:
                    logger.debug(f"Identified likely year sheet: {year_names[k[0]]}")
                    year_dict[year] = year_names[k[0]]
                elif len(k)==0:
                    # Check for typo
                    m = [y for y in year_names if {x for x in str(year)}=={x for x in y} and y not in year_dict.values()]
                    if len(m)!=1:
                        raise ValueError("Unable to parse sheet names")
                    year_dict[year] = m[0]
                else:
                    raise ValueError("Unable to parse sheet names")

            if years_found:
                logger.debug("Treating Excel file as different years stored in separate Excel sheets.")
                return year_dict, True

        return names, False


    def load(self, year=None, nrows=None, offset=0, *, agency=None, format_date=True, **kwargs):
        '''Download Excel file to pandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested. None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the Excel
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        agency : str
            (Optional) Name of the agency to filter for. None value returns data for all agencies.
        format_date : bool, optional
            If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
            to be pandas datetimes (or pandas Period in rare cases), by default True
            
        Returns
        -------
        pandas DataFrame
            DataFrame containing table imported from Excel file

        Note: Older Excel files (.xls) and OpenDocument file formats (.odf, .ods, .odt) are not supported. Please submit an issue if this is needed.
        '''

        logger.debug(f"Loading file from {self.url}")
        nrows_read = offset+nrows if nrows is not None else None
        sheets, has_year_sheets = self.__get_sheets()

        if has_year_sheets:
            if year==None:
                year = list(sheets.keys())
                year.sort()
                year = [year[0], year[-1]]
            if not isinstance(year, list):
                year = [year, year]

            table = pd.DataFrame()
            cols_added = 0
            for y in range(year[0], year[1]+1):
                if y in sheets:
                    logger.debug(f"Loading data from sheet {sheets[y]}")
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=UserWarning, message='Data validation extension is not supported')
                        df = pd.read_excel(self.excel_file, nrows=nrows_read, sheet_name=sheets[y])

                    df = self.__clean(df, sheets[y], has_year_sheets)

                    if len(table)==0:
                        table = df
                        col_matches = [[k] for k in range(len(df.columns))]
                    else:
                        if not df.columns.equals(table.columns):
                            # Conditional for preventing column names from being too different
                            if len(df.columns)+cols_added == len(table.columns) and \
                                (df.columns == table.columns[:len(df.columns)]).sum()>=len(df.columns)-3-cols_added:
                                # Try to find a typo
                                for m in [j for j in range(len(df.columns)) if table.columns[j]!=df.columns[j]]:
                                    for k in col_matches[m]:
                                        if table.columns[k]==df.columns[m]:
                                            break
                                        try:
                                            from rapidfuzz import fuzz
                                        except:
                                            raise ImportError(f"{self.url} requires installation of rapidfuzz " + 
                                                "(https://pypi.org/project/rapidfuzz/) to load data from multiple years (pip install rapidfuzz)")

                                        if fuzz.ratio(table.columns[k], df.columns[m]) > 80 or \
                                            fuzz.token_sort_ratio(table.columns[k], df.columns[m])>90:
                                            warnings.warn(f"Identified difference in column names when combining sheets {sheets[y-1]} and {sheets[y]}. " + 
                                                f"Column names are '{table.columns[k]}' and '{df.columns[m]}'. This appears to be a typo. " + 
                                                f"These columns are assumed to be the same and will be combined as column '{table.columns[k]}'")
                                            df.columns = [table.columns[k] if j==m else df.columns[j] for j in range(len(df.columns))]
                                            break
                                    else:
                                        warnings.warn(f"Column '{table.columns[m]}' in current DataFrame does not match '{df.columns[m]}' in new DataFrame. "+ 
                                            "When they are concatenated, both columns will be included.")
                                        col_matches[m].append(len(table.columns))
                                        cols_added+=1
                                        # raise ValueError(f"Column {table.columns[k]} in table does not match {df.columns[k]} in df")
                            else:
                                raise ValueError("Columns don't match")
                        logger.debug("Concatenating data from multiple year sheets")
                        table = pd.concat([table, df], ignore_index=True)

                    if nrows_read!=None and len(table)>=nrows_read:
                        break
        else:
            sheets_load = self.sheet.split("&") if isinstance(self.sheet,str) else [self.sheet]
            dfs = []
            for s in sheets_load:
                if isinstance(s,str):
                    s = s.strip()
                    if '*' in s:
                        p = s.replace('*','.*')
                        s = [x for x in sheets if re.search(p,x)]
                        assert len(s)==1
                        s = s[0]
                
                self.__check_sheet(s, sheets)
                sheet_name = 0 if s is None else s
                logger.debug(f"Loading sheet: {sheet_name}")
                table = pd.read_excel(self.excel_file, nrows=nrows_read, sheet_name=sheet_name)
                dfs.append(table)
            table = pd.concat(dfs, ignore_index=True)
            table = self.__clean(table)               

        # Check for empty rows at the bottom
        num_empty = table.isnull().sum(axis=1)
        empty_rows = num_empty==len(table.columns)
        if empty_rows.any():
            # Check if all rows after first empty row are empty or almost empty
            empty_rows = empty_rows[empty_rows]
            num_empty = num_empty.loc[empty_rows.index[0]:]
            if ((num_empty / len(table.columns)) > 0.75).all():
                logger.debug(f"Detected empty rows at the bottom of the table. Keeping the first {empty_rows.index[0]} rows")
                table = table.head(empty_rows.index[0])

        # Clean up column names
        table.columns = [x.strip() if isinstance(x, str) else x for x in table.columns]

        table = filter_dataframe(table, date_field=self.date_field, year_filter=year, 
            agency_field=self.agency_field, agency=agency, format_date=format_date)
        
        if offset>0:
            rows_limit = nrows_read if nrows_read is not None and nrows_read<len(table) else len(table)
            logger.debug(f"Extracting {rows_limit} rows starting at {offset}")
            table = table.iloc[offset:rows_limit].reset_index(drop=True)
        if nrows is not None and len(table)>nrows:
            logger.debug(f"Extracting the first {nrows} rows")
            table = table.head(nrows)

        return table


    def __check_sheet(self, cur_sheet, sheets):
        if cur_sheet is None:
            if not all([re.match(r"Sheet\d+",x) for x in sheets[1:]]):
                # More than 1 sheet has non-default name so can't assume 1st sheet
                raise ValueError(f"The Excel file at {self.url} has {len(sheets)} sheets but no dataset id is specified to indicate which to use.")
        elif cur_sheet not in sheets:
            raise ValueError(f"Sheet {cur_sheet} not found in Excel file at {self.url}")


    def __find_column_names(self, df):
        # Check if the entire column is null for any unnamed columns
        unnamed_cols = [x for x in df.columns if pd.isnull(x) or 'Unnamed' in x]
        delete_cols_tf = df[unnamed_cols].apply(lambda x: [pd.isnull(y) or (isinstance(y,str) and len(y.strip())==0) for y in x]).mean()==1  # Null or empty string
        delete_cols = delete_cols_tf.index[delete_cols_tf]
        if len(delete_cols)>0 and len(delete_cols)!=len(delete_cols_tf):
            m = (~delete_cols_tf).sum()
            if len(delete_cols_tf)<15 or (~delete_cols_tf[-m:]).sum()!=m>2:
                raise NotImplementedError(f'Unable to parse columns in {self.url}')
            # Only non-null unnamed columns are far to the right of the last named column
            delete_cols_tf[-m:] = True
            delete_cols = delete_cols_tf.index[delete_cols_tf]
        elif len(unnamed_cols)>0 and len(delete_cols)==0 and all(['Unnamed' in x for x in df.columns[-len(unnamed_cols):]]) and \
            (df[df.columns[-len(unnamed_cols):]].notnull().sum()<=1).all():
            # All unnamed columns have 0 or 1 non-null value
            delete_cols = unnamed_cols

        unnamed_cols = [x for x in unnamed_cols if x not in delete_cols]
        df = df.drop(columns=delete_cols)

        if len(unnamed_cols)==0:
            return df
        
        if len(delete_cols)>0 and len(unnamed_cols)>0:
            raise NotImplementedError(f"Unable to parse columns in {self.url}")
        
        # Find 1st unnamed column
        for idx_unnamed, x in enumerate(df.columns):
            if 'Unnamed' in x:
                break

        if idx_unnamed==0 and len(unnamed_cols)==1 and all(x==k+1 for k,x in enumerate(df[unnamed_cols[0]])):
            # First column is just row numbers
            df = df.iloc[:,1:]
        elif idx_unnamed < 2 and all(['Unnamed' in x for x in df.columns[idx_unnamed:]]):
            # First row is likely just some information about the table
            # First columns are likely in first row of data
            col_row = 0
            new_cols = [x for x in df.iloc[0]]
            while not all([isinstance(x, str) or pd.isnull(x) for x in new_cols]) or all([pd.isnull(x) for x in new_cols[1:]]):  # First column often has text while rest don't in non-data sections
                col_row+=1
                new_cols = [x for x in df.iloc[col_row]]
            
            new_cols = [x.strip() if isinstance(x,str) else x for x in new_cols]
            
            logger.debug(f"Detected that first row does not contain column headers: {df.columns}")
            logger.debug(f"Making the  second row the column headers: {new_cols}")
            df.columns = new_cols
            df = df.iloc[col_row+1:]

            # Look for rows that are just the column names to find if there are multiple tables in the sheet
            not_col_names = df.apply(lambda x: not all([y==df.columns[k] for k,y in enumerate(x)]), axis=1)
            if not not_col_names.all():
                df = df[not_col_names]
                # Look for gaps between tables and/or tables that don't contain any data (including ones with a row that just says there is no data)
                df = df[df.iloc[:,2:].notnull().any(axis=1)]
        else:
            # This is likely the result of column headers that span multiple rows
            # with some headers in the first row in merged Excel columns that will not 
            # be merged in the pandas columns

            # Validate that columns match expected pattern
            for k, c in enumerate(df.columns):
                if 'Unnamed' in c:
                    if pd.isnull(df.loc[0,c]): # If the column name is empty, the first row should be part of the column name
                        raise ValueError(f"Unexpected condition in column {c} where first row is null for url {self.url}")
                elif pd.notnull(df.loc[0,c]):  # Both column name and 1st row have value. Column name is expected to be a merged column
                    if k == len(df.columns) or 'Unnamed' not in df.columns[k+1]:
                        raise ValueError(f"Unexpected column pattern with columns {df.columns} and first row {df.iloc[0]} for url {self.url}")
                    
            # Merge 1st row with columns
            new_cols = []
            addon = ''
            for c in df.columns:
                if pd.isnull(df.loc[0,c]):
                    addon = ''
                    new_cols.append(c)
                elif c.lower().endswith('info'):
                    addon = re.sub(r'[Ii]nfo', '', c).strip() + ' '
                    new_cols.append(addon + df.loc[0,c])
                else:
                    new_cols.append(addon + df.loc[0,c])

            df = df.copy() # Avoids any warnings from pandas
            df.columns = new_cols
            df = df.iloc[1:]

        df = df.reset_index(drop=True)
        return df

        # Row names may not be the 1st row in which case columns need to be fixed
        # max_drops = 5
        # num_drops = 0
        # updated_cols = False
        # while sum([(pd.isnull(x) or "Unnamed" in x) for x in df.columns]) / len(df.columns) > 0.5:
        #     if ((m:=df.isnull().mean())==1).any():
        #         keep = []
        #         found1 = False
        #         num1 = 0
        #         max1 = 3
        #         for k,v in m.items():
        #             if found1 and v!=1:
        #                 raise ValueError(f"Unable to parse Excel table from {self.url}")
        #             elif v==1:
        #                 found1 = True
        #                 num1+=1
        #                 if num1>=max1:
        #                     break
        #             else:
        #                 keep.append(k)
        #         df = df[keep]
        #     else:
        #         new_cols = [x for x in df.iloc[0]]
        #         if all([isinstance(x, str) or pd.isnull(x) for x in new_cols]):
        #             logger.debug(f"Detect that first row does not contain column headers: {df.columns}")
        #             logger.debug(f"Making the  second row the column headers: {new_cols}")
        #             df.columns = new_cols
        #             df.drop(index=df.index[0], inplace=True)
        #             df.reset_index(drop=True, inplace=True)
        #             num_drops+=1
        #             updated_cols = True

        #             if len(df)==0 or num_drops>=max_drops:
        #                 raise ValueError("Unable to find column names")
                    
        # if sum([(pd.isnull(x) or "Unnamed" in x) for x in df.columns]) / len(df.columns) > 0.3 and \
        #     len(df)>0:
        #     # Check for multi-row column header with merged columns in first row of spreadsheet
        #     is_multi_row = True
        #     for k, c in enumerate(df.columns):
        #         if 'Unnamed' in c:
        #             if pd.isnull(df.loc[0,c]):
        #                 raise ValueError(f"Unexpected condition in column {c} where first row is null")
        #         elif pd.notnull(df.loc[0,c]):
        #             if k == len(df.columns) or 'Unnamed' not in df.columns[k+1]:
        #                 raise ValueError(f"Unexpected column pattern with columns {df.columns} and first row {df.iloc[0]}")
                    
        #     if is_multi_row:
        #         # Merge 1st row with columns
        #         new_cols = []
        #         addon = ''
        #         for k, c in enumerate(df.columns):
        #             if pd.isnull(df.loc[0,c]):
        #                 addon = ''
        #                 new_cols.append(c)
        #             elif c.lower().endswith('info'):
        #                 addon = re.sub(r'[Ii]nfo', '', c).strip() + ' '
        #                 new_cols.append(addon + df.loc[0,c])
        #             else:
        #                 new_cols.append(addon + df.loc[0,c])

        #         df = df.copy() # Avoids any warnings from pandas
        #         df.columns = new_cols
        #         df = df.iloc[1:]

        # if updated_cols:
        #     # Remove any empty rows or repeated column headers. There may be multiple of the same table for different years
        #     df = df.dropna(thresh=3)
        #     df = df[df.apply(lambda x: not all([y==df.columns[k] for k,y in enumerate(x)]), axis=1)]


    def __clean(self, df, sheet_name=None, has_year_sheets=False):
        if any([(pd.isnull(x) or "Unnamed" in x) for x in df.columns]):
            # At least 1 column name was empty
            df = self.__find_column_names(df)

        if has_year_sheets:
            if sheet_name and 'Year' not in df:
                df['Year'] = int(sheet_name)
            if sheet_name and sheet_name.isdigit() and 1990 < int(sheet_name) < 2100 and \
                'Month' in df and 'Day' in df:
                # Rearrange columns
                col_order = []
                year_added = False
                day_found = False
                for c in df.columns:
                    if not year_added and c=='Month':
                        if day_found:
                            col_order.append(c)
                            col_order.append('Year')
                        else:
                            col_order.append('Year')
                            col_order.append(c)
                        year_added = True
                    elif c=='Day':
                        day_found = True
                        col_order.append(c)
                    elif c!='Year':
                        col_order.append(c)
                df = df[col_order]

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=RuntimeWarning)
            df = df.convert_dtypes()

        # Check for 1st column being row numbers
        if pd.isnull(df.columns[0]) and list(df.iloc[:,0]) in [[k+1 for k in range(0, len(df))], [k for k in range(0, len(df))]]:
            logger.debug("Removing the 1st column which is just the row number")
            df = df.iloc[:, 1:]

        return df
        

    def get_years(self, *, force=False, **kwargs):
        '''Get years contained in data set
        
        Parameters
        ----------
        force : bool
            (Optional) If false, an exception will be thrown. It may be more efficient to load the table and extract years manually
            
        Returns
        -------
        list
            list containing years in data set
        '''

        sheets, has_year_sheets = self.__get_sheets()

        if has_year_sheets:
            years = list(sheets.keys())
            years.sort()
            return list(sheets.keys())
        if not force:
            raise ValueError("Extracting the years of a Excel file requires reading the whole file in. In most cases, "+
                "running load() with no arguments to load in the whole CSV file and manually finding the years will be more "
                "efficient. If running get_years is still desired, set force=True")
        else:
            if self.date_field==None:
                raise ValueError("No date field provided to access year information")
            df = self.load()
            if is_datetime(df[self.date_field]):
                years = list(df[self.date_field].dt.year.dropna().unique())
            elif is_numeric_dtype(df[self.date_field]):
                years = list(df[self.date_field].dropna().unique())
            else:
                raise TypeError("Unknown date column format")
            return [int(x) for x in years]


class Arcgis(Data_Loader):
    """
    A class for accessing data from ArcGIS clients

    Parameters
    ----------
    url : str
        URL
    date_field : str
        Name of the column that contains the date
    max_record_count : int
        Maximum number of records that can be returned per request
    is_table : bool
        Indicates if dataset is a table. Tables do not have GPS data

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True)
        Load data for query
    get_count(year=None, where=None)
        Get number of records/rows generated by query
    get_years()
        Get years contained in data set
    """

    # Based on https://developers.arcgis.com/rest/services-reference/online/feature-layer.htm
    __max_maxRecordCount = 32000
    
    def __init__(self, url, date_field=None):
        '''Create Arcgis object

        Parameters
        ----------
        url : str
            URL for ArcGIS data
        date_field : str
            (Optional) Name of the column that contains the date
        '''

        self._date_type = None
        self._date_format = None
        self.date_field = date_field
        self.verify = False

        # Table vs. Layer: https://developers.arcgis.com/rest/services-reference/enterprise/layer-feature-service-.htm
        # The layer resource represents a single feature layer or a nonspatial table in a feature service. 
        # A feature layer is a table or view with at least one spatial column.
        # For tables, it provides basic information about the table such as its ID, name, fields, types, and templates. 
        # For feature layers, in addition to the table information, it provides information such as its geometry type, min and max scales, and spatial reference.

        p = re.search(r"(MapServer|FeatureServer)/\d+", url)
        self.url = url[:p.span()[1]]

        # Get metadata
        meta = self.__request()

        if 'type' not in meta and meta['status']=='error':
            raise OPD_DataUnavailableError(self.url, meta['messages'], _url_error_msg.format(self.url))

        if "maxRecordCount" in meta:
            self.max_record_count = meta["maxRecordCount"] if meta['maxRecordCount']<self.__max_maxRecordCount else self.__max_maxRecordCount
        else:
            self.max_record_count = None

        if meta["type"]=="Feature Layer":
            self.is_table = False
        elif meta["type"]=="Table":
            self.is_table = True
        else:
            raise ValueError("Unexpected ArcGIS layer type: {}".format(meta["type"]))

        self.__set_verify(_verify_arcgis)


    def isfile(self):
        '''Returns False to indicate that ArcGIS data is not file-based

        Returns
        -------
        False
        '''
        return False


    def __set_verify(self, verify):
        # Sets whether to validate OPD queries against ones using arcgis package
        if not verify:
            self.verify = verify
        else:
            # https://developers.arcgis.com/python/
            try:
                from arcgis.features import FeatureLayerCollection
                self.verify =verify
            except:
                self.verify = False
                return

            last_slash =self.url.rindex("/")
            layer_num = self.url[last_slash+1:]
            base_url = self.url[:last_slash]
            try:
                layer_collection = FeatureLayerCollection(base_url)
            except Exception as e:
                if len(e.args)>0:
                    if "Error Code: 500" in e.args[0]:
                        raise OPD_DataUnavailableError(self.url, e.args, _url_error_msg.format(self.url))
                    elif "A general error occurred: 'authInfo'" in e.args[0]:
                        raise OPD_arcgisAuthInfoError(self.url, e.args, _url_error_msg.format(self.url))
                raise e
            except: raise

            is_table = True
            self.__active_layer = None
            for layer in layer_collection.layers:
                layer_url = layer.url
                if layer_url[-1] == "/":
                    layer_url = layer_url[:-1]
                if layer_num == layer_url[last_slash+1:]:
                    self.__active_layer = layer
                    is_table = False
                    break

            if is_table != self.is_table:
                raise ValueError("is_table is not read in properly")

            if self.is_table:
                for layer in layer_collection.tables:
                    layer_url = layer.url
                    if layer_url[-1] == "/":
                        layer_url = layer_url[:-1]
                    if layer_num == layer_url[last_slash+1:]:
                        self.__active_layer = layer
                        break

            if self.__active_layer == None:
                raise ValueError("Unable to find layer")


    def get_count(self, year=None, *,  where=None, **kwargs):
        '''Get number of records for a Arcgis data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        where : str
            (Optional) SQL where query
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''
        
        if self._last_count is not None and self._last_count[0]==(year,where):
            logger.debug("Request matches previous count request. Returning saved count.")
            record_count = self._last_count[1]
            where_query = self._last_count[2]
        elif where==None:
            where_query, record_count = self.__construct_where(year)

            if not self.__accurate_count:
                raise ValueError(f"Count is not accurate for year input {self.__accurate_count}. "
                                 "Date field contains data in text format not date format "
                                 "and the text not formatted in a way that makes getting a count "
                                 "possible without loading in the data. Either adjust the input to "
                                 "get_count to get a range of years instead of a range of dates or "
                                 "load in the data for the current date range")
        else:
            where_query = where
            try:
                record_count = self.__request(where=where, return_count=True)["count"]
                if self.verify:
                    record_count_orig = self.__active_layer.query(where=where, return_count_only=True)
                    if record_count_orig!=record_count:
                        raise ValueError(f"Record count of {record_count} does not equal count from arcgis package of {record_count_orig}")
            except Exception as e:
                if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                    raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.url))
                else:
                    raise
            except:
                raise

        self._last_count = ((year,where), record_count, where_query)

        return record_count


    def __request(self, where=None, return_count=False, out_fields="*", out_type="json", offset=0, count=None, sp_ref=None):
        
        # Running with no inputs or just an out_type will return metadata only
        url = self.url + "/"
        params = {}
        if where != None:
            url+="query"
            params["where"] = where
            params["outFields"] = out_fields
            if return_count:
                params["returnCountOnly"] = True
            else:
                # Don't add offset for returning record count. The maximum value returned appears to be the maxRecordCount not the total count of records.
                # If it's ever desired to get the record with an offset, recommend getting the record count without the offset and then subtracting the offset.
                params["resultOffset"] = offset
                if sp_ref!=None:
                    params["outSR"] = sp_ref
                if self.date_field!=None:
                    params["orderByFields"] = self.date_field
                
            if count!=None:
                params["resultRecordCount"] = count

        params["f"] = out_type
        params["cacheHint"] = False

        logger.debug(f"Request data from {url}")
        for k,v in params.items():
            logger.debug(f"\t{k} = {v}")

        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
        except requests.exceptions.SSLError as e:
            if "[SSL: UNSAFE_LEGACY_RENEGOTIATION_DISABLED] unsafe legacy renegotiation disabled" in str(e.args[0]):
                with get_legacy_session() as session:
                    r = session.get(url, params=params)
                    
                r.raise_for_status()
            else:
                raise e
        except requests.HTTPError as e:
            if len(e.args)>0:
                if "503 Server Error" in e.args[0]:
                    raise OPD_DataUnavailableError(self.url, e.args, _url_error_msg.format(self.url))

            else: raise e
        except requests.exceptions.ConnectTimeout as e:
            raise OPD_DataUnavailableError(self.url, e.args, _url_error_msg.format(self.url))
        except Exception as e: 
            raise e

        result = r.json()

        if isinstance(result, dict) and len(result.keys()) and "error" in result:
            args = ()
            for k,v in result['error'].items():
                if not hasattr(v,'__len__') or len(v)>0:
                    v = v[0] if isinstance(v,list) and len(v)==1 else v
                    args += (k,v)
            raise OPD_DataUnavailableError(url, 'Error returned by ArcGIS query', *args, _url_error_msg.format(self.url))
        
        return result


    def __construct_where(self, year=None, date_range_error=True):
        where_query = ""
        self.__accurate_count = True
        if self._last_count is not None and self._last_count[0]==(year,None):
            record_count = self._last_count[1]
            where_query = self._last_count[2]
        elif self.date_field!=None and year!=None:
            where_query, record_count = self._build_date_query(year, date_range_error)
        else:
            where_query = '1=1'
            try:
                record_count = self.__request(where=where_query, return_count=True)["count"]
                if self.verify:
                    record_count_orig = self.__active_layer.query(where=where_query, return_count_only=True)
                    if record_count_orig!=record_count:
                        raise ValueError(f"Record count of {record_count} does not equal count from arcgis package of {record_count_orig}")
            except Exception as e:
                if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                    raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.url))
                else:
                    raise
            except:
                raise

        if self.__accurate_count:
            # Count may not be accurate if date ranges are allowed and the date field was a string
            self._last_count = ((year,None), record_count, where_query)

        return where_query, record_count
    
    def _build_date_query(self, year, date_range_error):
        # Determine format by getting some data
        data = None
        if not self._date_type:
            data = self.__request(where='1=1', out_fields=self.date_field, count=1000)
            self._date_type = data['fields'][0]['type']

        if self._date_type=='esriFieldTypeDate':
            where_query, record_count = self._build_date_query_date_type(year)
        elif self._date_type=='esriFieldTypeString':
            where_query, record_count = self._build_date_query_string_type(year, data, date_range_error)
        elif self._date_type in ['esriFieldTypeInteger','esriFieldTypeDouble'] and \
            (self.date_field.lower()=='yr' or 'year' in self.date_field.lower()):
            where_query, record_count = self._build_date_query_date_type(year, is_numeric_year=True)
        else:
            raise NotImplementedError(f"Unknown field {self._date_type}")
        
        return where_query, record_count

    def _build_date_query_string_type(self, year, data, date_range_error):
        if data != None:
            dates = [x['attributes'][self.date_field] for x in data['features']]

            # [regex for pattern, Arcgis Pattern OR date delimiter (punctuation between numbers), whether to use inquality to do comparison,
                # OPTIONAL time delimiter]
            @dataclass
            class DateParseParams:
                regex_pattern: re.Pattern
                arcgis_pattern: Optional[str] = None
                ineq_comp: bool = False
                date_delim: str = ""
                full_date: bool = True

            matches = [
                DateParseParams(re.compile(r"^(19|20)\d{6}\b"), ineq_comp=True),  # YYYYMMDD
                DateParseParams(re.compile(r"^(19|20)\d{12}\b"), ineq_comp=True),  # YYYYMMDDHHMMSS
                DateParseParams(re.compile(r"^(19|20)\d{2}-\d{2}-\d{2}(\b|T)"), ineq_comp=True, date_delim="-"),  # YYYY-MM-DDThh:mm:ss
                DateParseParams(re.compile(r"^[A-Z][a-z]+ \d{1,2}, (19|20)\d{2}\b"), "{} LIKE '[A-Z]% [0-9][0-9], {}' OR {} LIKE '[A-Z]% [0-9], {}'"),  # Month DD, YYYY
                DateParseParams(re.compile(r"^\d{1,2}[-/]\d{1,2}[-/](19|20)\d{2}\b"), 
                    "{} LIKE '%[0-9][0-9][/-][0-9][0-9][/-]{}%' OR {} LIKE '%[0-9][/-][0-9][0-9][/-]{}%' OR " + 
                    "{} LIKE '%[0-9][/-][0-9][/-]{}%' OR {} LIKE '%[0-9][0-9][/-][0-9][/-]{}%'"),  # mm/dd/yyyy or mm-dd-yyyy
                DateParseParams(re.compile(r"^\d{4}[-/]\d{1,2}$"), "{} LIKE '{}[-/][0-9][0-9]' OR {} LIKE '{}[-/][0-9]'", full_date=False),  # YYYY-MM or YYYY/M
                DateParseParams(re.compile(r"^\d{4}$"), "{} = '{}'", full_date=False),  # YYYY
                DateParseParams(re.compile(r"^\d{1,2}[-/]\d{4}$"), "{} LIKE '[0-9][0-9][-/]{}' OR {} LIKE '[0-9][-/]{}'", full_date=False),  # MM-YYYY or MM/YYYY
            ]

            hi = 0.0
            idx = None
            for k, m in enumerate(matches):
                if (new:=sum([isinstance(x,str) and m.regex_pattern.search(x) != None for x in dates])/len(dates)) > hi:
                    hi = new
                    idx = k

            if hi < 0.9:
                raise ValueError("Unable to find date string pattern")
            
            self._ineq_comp = matches[idx].ineq_comp
            if self._ineq_comp:
                self._date_delim = matches[idx].date_delim
            else:
                self._date_format = repeat_format(matches[idx].arcgis_pattern)
                self._full_date = matches[idx].full_date

        if self._ineq_comp:
            where_query, record_count = self._build_date_query_date_type(year, self._date_delim, is_date_string=True)
        else:
            year = [year] if isinstance(year, numbers.Number) else year.copy()
            for k,y in enumerate(year):
                if isinstance(y,str) and re.search(r'^\d{4}-\d{2}-\d{2}', y):
                    year[k] = y[:4]
                    self.__accurate_count = False
                    
            if (not self._full_date or date_range_error) and any([isinstance(x,str) and len(x)!=4 for x in year]):
                # Currently can only handle years
                raise ValueError(f"Date column is a string data type at the source {self.url}. "+
                                "Currently only able to filter for a single year (2023) or a year range ([2022,2023]) "+
                                "not a date range ([2022-01-01, 2022-03-01]).")
            
            where_query = self._date_format.format(self.date_field, year[0])
            if len(year)>1:
                for x in range(int(year[0])+1,int(year[1])+1):
                    where_query = f"{where_query} or " + self._date_format.format(self.date_field, x)

            record_count = self.__request(where=where_query, return_count=True)["count"]

        return where_query, record_count
        
        

    def _build_date_query_date_type(self, year, date_delim='-', is_numeric_year=False, is_date_string=False):
        # List of error messages that can occur for bad queries as we search for the right query format
        query_err_msg = ["Unable to complete operation", "Failed to execute query", "Unable to perform query", "Database error has occurred", 
                         "'where' parameter is invalid", "Parsing error",'Query with count request failed']
        
        where_query = ""
        zero_found = False
        if self._date_format in [0,1] or self._date_format==None:
            start_date, stop_date = _process_date(year, force_year=is_numeric_year, is_date_string=is_date_string)

            if date_delim=='':
                start_date = start_date.replace('-','')
                stop_date = stop_date.replace('-','')
            elif date_delim!='-':
                raise NotImplementedError("Unable to handle this delimiter")
            
            for k in range(0,2):
                if self._date_format is not None and self._date_format!=k:
                    continue
                if k==0:
                    if is_numeric_year:
                        where_query = f"{self.date_field} >= {start_date} AND  {self.date_field} <= {stop_date}"
                    else:
                        where_query = f"{self.date_field} >= '{start_date}' AND  {self.date_field} <= '{stop_date}'"
                elif is_numeric_year:
                    break
                else:
                    # break
                    # Dataset (San Jose crash data) that required this does not function well so removing its functionality for now to speed up this function.
                    # This is the recommended way but it has been found to not work sometimes. One dataset was found that requires this.
                    # https://gis.stackexchange.com/questions/451107/arcgis-rest-api-unable-to-complete-operation-on-esrifieldtypedate-in-query
                    stop_date_tmp = stop_date.replace("T"," ")
                    where_query = f"{self.date_field} >= TIMESTAMP '{start_date}' AND  {self.date_field} < TIMESTAMP '{stop_date_tmp}'"
            
                try:
                    record_count = self.__request(where=where_query, return_count=True)["count"]

                    if self.verify:
                        record_count_orig = self.__active_layer.query(where=where_query, return_count_only=True)
                        if record_count_orig!=record_count:
                            raise ValueError(f"Record count of {record_count} does not equal count from arcgis package of {record_count_orig}")
                    if self._date_format!=None or record_count>0:
                        self._date_format = k
                        return where_query, record_count
                    else:
                        zero_found = True
                except Exception as e:
                    if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                        raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.url))
                    elif any([x in y for x,y in itertools.product(query_err_msg, e.args) if isinstance(y,str)]):
                        # This query throws an error for this dataset. Try another one below
                        pass
                    else:
                        raise
                except:
                    raise

        if not zero_found:
            raise AttributeError(f"Unable to find date format for {self.url}")

        return "", 0
    
    def _build_date_query_old(self, year):

        # List of error messages that can occur for bad queries as we search for the right query format
        query_err_msg = ["Unable to complete operation", "Failed to execute query", "Unable to perform query", "Database error has occurred", 
                         "'where' parameter is invalid", "Parsing error",'Query with count request failed']
        
        where_query = ""
        zero_found = False
        if self._date_format in [0,1] or self._date_format==None:
            start_date, stop_date = _process_date(year)
            
            for k in range(0,2):
                if self._date_format is not None and self._date_format!=k:
                    continue
                if k==0:
                    where_query = f"{self.date_field} >= '{start_date}' AND  {self.date_field} < '{stop_date}'"
                else:
                    # break
                    # Dataset (San Jose crash data) that required this does not function well so removing its functionality for now to speed up this function.
                    # This is the recommended way but it has been found to not work sometimes. One dataset was found that requires this.
                    # https://gis.stackexchange.com/questions/451107/arcgis-rest-api-unable-to-complete-operation-on-esrifieldtypedate-in-query
                    stop_date_tmp = stop_date.replace("T"," ")
                    where_query = f"{self.date_field} >= TIMESTAMP '{start_date}' AND  {self.date_field} < TIMESTAMP '{stop_date_tmp}'"
            
                try:
                    record_count = self.__request(where=where_query, return_count=True)["count"]

                    if self.verify:
                        record_count_orig = self.__active_layer.query(where=where_query, return_count_only=True)
                        if record_count_orig!=record_count:
                            raise ValueError(f"Record count of {record_count} does not equal count from arcgis package of {record_count_orig}")
                    if self._date_format!=None or record_count>0:
                        self._date_format = k
                        return where_query, record_count
                    else:
                        zero_found = True
                except Exception as e:
                    if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                        raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.url))
                    elif len(e.args)>0 and (any([x in e.args[0] for x in query_err_msg]) or any([x in e.args[-1] for x in query_err_msg])):
                        # This query throws an error for this dataset. Try another one below
                        pass
                    else:
                        raise
                except:
                    raise


        where_formats = [
            "{} LIKE '%[0-9][0-9][/-][0-9][0-9][/-]{}%' OR {} LIKE '%[0-9][/-][0-9][0-9][/-]{}%' OR " + 
                "{} LIKE '%[0-9][/-][0-9][/-]{}%' OR {} LIKE '%[0-9][0-9][/-][0-9][/-]{}%'",   # mm/dd/yyyy or mm-dd-yyyy
            "{} LIKE '{}/[0-9][0-9]' OR {} LIKE '{}/[0-9]'",                # yyyy/mm
            "{} = {}",                # yyyy
            "{} LIKE '[0-9][0-9]-{}' OR {} LIKE '[0-9]-{}'",   # mm-yyyy or m-yyyy
            "{} = '{}'",                # 'yyyy'
            "{}>='{}0101' AND {}<='{}1231'",            # yyyymmdd as float
            "{} LIKE '[A-Z]% [0-9][0-9], {}'"   # {Month Name} dd, yyyy
        ]
        where_formats = [repeat_format(x) for x in where_formats]

        # Make year iterable
        year = [year] if isinstance(year, numbers.Number) else year

        if self._date_format not in [None, 0, 1] and any([isinstance(x,str) and len(x)!=4 for x in year]):
            # Currently can only handle years
            raise ValueError("Currently unable to handle non-year inputs")

        for format in where_formats:
            if self._date_format==format or self._date_format==None:
                where_query = format.format(self.date_field, year[0])
                for x in year[1:]:
                    where_query = f"{where_query} or " + format.format(self.date_field, x)

                try:
                    record_count = self.__request(where=where_query, return_count=True)["count"]

                    if self.verify:
                        record_count_orig = self.__active_layer.query(where=where_query, return_count_only=True)
                        if record_count_orig!=record_count:
                            raise ValueError(f"Record count of {record_count} does not equal count from arcgis package of {record_count_orig}")
                    if self._date_format!=None or record_count>0:
                        self._date_format = format
                        return where_query, record_count
                    else:
                        zero_found = True
                except Exception as e:
                    if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                        raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.url))
                    elif len(e.args)>0 and (any([x in e.args[0] for x in query_err_msg]) or any([x in e.args[-1] for x in query_err_msg])):
                        # This query throws an error for this dataset. Try another one below
                        pass
                    else:
                        raise
                except:
                    raise

        if not zero_found:
            raise AttributeError(f"Unable to find date format for {self.url}")

        return "", 0

    
    def load(self, year=None, nrows=None, offset=0, *, pbar=True, format_date=True, **kwargs):
        '''Download table from ArcGIS to pandas or geopandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the table
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        pbar : bool
            (Optional) If true (default), a progress bar will be displayed
        format_date : bool, optional
            If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
            to be pandas datetimes (or pandas Period in rare cases), by default True
            
        Returns
        -------
        pandas or geopandas DataFrame
            DataFrame containing table imported from ArcGIS
        '''
        
        where_query, record_count = self.__construct_where(year, date_range_error=False)
        
        # Update record count for request record offset
        record_count-=offset
        if record_count<=0:
            return pd.DataFrame()

        batch_size = self.max_record_count or _default_limit
        nrows = nrows if nrows!=None and record_count>=nrows else record_count
        batch_size = nrows if nrows < batch_size else batch_size
        num_batches = ceil(nrows / batch_size)
            
        pbar = pbar and num_batches>1
        if pbar:
            bar = tqdm(desc=self.url, total=nrows, leave=False) 
            
        features = []
        for batch in range(num_batches):
            bs = batch_size if batch<num_batches-1 else nrows-batch*batch_size
            try:
                data = self.__request(where=where_query, offset=offset+batch*batch_size, count=bs)
                features.extend(data["features"])
                if self.verify:
                    layer_query_result_old = self.__active_layer.query(where=where_query, result_offset=batch*batch_size, 
                        result_record_count=batch_size, return_all_records=False)
                    sdf = layer_query_result_old.sdf

                    attributes = pd.DataFrame.from_records([x["attributes"] for x in data["features"]])
                    for col in [x["name"] for x in data["fields"] if x["type"]=='esriFieldTypeDate']:
                        attributes[col] = to_datetime(attributes[col], unit="ms")
                    
                    if not self.is_table:
                        geom_old = sdf.pop("SHAPE")
                        has_point_geometry = any("geometry" in x and "x" in x["geometry"] for x in data["features"])
                        if not has_point_geometry and geom_old.apply(lambda x: x is not None).any():
                            raise KeyError("Geometry not found")
                        if _has_gpd and has_point_geometry:
                            x = [x["geometry"]["x"] if "geometry" in x else None for x in data["features"]]
                            y = [x["geometry"]["y"] if "geometry" in x else None for x in data["features"]]
                            x_old = [x["x"] if x!=None else None for x in geom_old]
                            y_old = [x["y"] if x!=None else None for x in geom_old]
                            if x!=x_old and any([x!=None for x in x_old]):
                                raise ValueError(f"X coordinates do not match for {self.url}")
                            if y!=y_old and any([x!=None for x in y_old]):
                                raise ValueError(f"Y coordinates do not match for {self.url}")

                    if not sdf.columns.equals(attributes.columns):
                        # A case was found where data from arcgis package had extra OBJECT_ID column (OBJECT_ID1 and OBJECT_ID)
                        # These columns are not used anyway so just remove them
                        missing_cols = [x for x in sdf.columns if x not in attributes.columns]
                        for col in missing_cols:
                            if col in ["OBJECTID"]:
                                sdf.pop(col)
                            else:
                                raise ValueError(f"Column '{col}' exists in arcgis query but not opd query")

                    if not sdf.equals(attributes):
                        raise ValueError(f"DataFrames do not match for {self.url}")

                if batch==0:
                    date_cols = [x["name"] for x in data["fields"] if x["type"]=='esriFieldTypeDate' and x['name'].lower()!='time']
                    if not self.is_table:
                        wkid = data["spatialReference"]["wkid"]
                    if len(data["features"]) not in [batch_size, nrows]:
                        num_rows = len(data["features"])
                        raise ValueError(f"Number of rows is {num_rows} but is expected to be max rows to read {batch_size} or total number of rows {nrows}")
            except Exception as e:
                if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                    raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.url))
                else:
                    raise
            except:
                raise

            if pbar:
                bar.update(len(data["features"]))

        if pbar:
            bar.close()

        df = pd.DataFrame.from_records([x["attributes"] for x in features])
        if format_date:
            for col in date_cols:
                if col in df:
                    logger.debug(f"Column {col} had a data type of esriFieldTypeDate. Converting values to datetime objects.")
                    df[col] = to_datetime(df[col], unit="ms", errors='coerce')

        if not self.__accurate_count:
            if not format_date:
                raise ValueError("Dates cannot be filtered if format_date is False for this dataset due to the date column not being a "+
                                 "esriFieldTypeDate type at the Arcgis source. Note: most other Arcgis datasets will work fine if format_date is False")
            logger.debug(f"User requested filtering by a date range but this was NOT done in the Arcgis query "+
                         f"due to the date field not being in a date format. Converting {self.date_field} column to "
                         f"a datetime in order to filter for requested date range {year}")
            df[self.date_field] = to_datetime(df[self.date_field], errors='coerce')
            date_range = [str(x) for x in year]
            if len(date_range[0])==4:
                date_range[0] = date_range[0]+'-01-01'
            if len(date_range[1])==4:
                date_range[1] = date_range[1] + "-12-31T23:59:59.999"
            else:
                date_range[1] = date_range[1] + "T23:59:59.999"

            df = df[ (df[self.date_field] >= date_range[0]) & (df[self.date_field] <= date_range[1]) ]

        if len(df) > 0:
            has_point_geometry = any("geometry" in x and "x" in x["geometry"] for x in features)
            if not self.is_table and has_point_geometry:
                if _use_gpd_force is not None:
                    if not _has_gpd and _use_gpd_force:
                        raise ValueError("User cannot force GeoPandas usage when it is not installed")
                    use_gpd = _use_gpd_force
                else:
                    use_gpd = _has_gpd

                if use_gpd:
                    # pyproj installs with geopandas
                    from pyproj.exceptions import CRSError
                    from pyproj import CRS

                    geometry = []
                    for feat in features:
                        if "geometry" not in feat:
                            geometry.append(None)
                        elif feat["geometry"]["x"]=="NaN":
                            geometry.append(Point(nan, nan))
                        else:
                            geometry.append(Point(feat["geometry"]["x"], feat["geometry"]["y"]))

                    logger.debug("Geometry found. Contructing geopandas GeoDataFrame")
                    try:
                        df = gpd.GeoDataFrame(df, crs=wkid, geometry=geometry)
                    except CRSError:
                        # CRS input method recommended by pyproj team to deal with CRSError for wkid = 102685
                        crs = CRS.from_authority("ESRI", wkid)
                        df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
                    except Exception as e:
                        raise e
                else:
                    geometry = [feat["geometry"] if "geometry" in feat else None for feat in features]

                    if "geolocation" not in df:
                        logger.debug("Adding geometry column generated from spatial data provided by request.")
                        df["geolocation"] = geometry

            return df
        else:
            return pd.DataFrame()


class Carto(Data_Loader):
    """
    A class for accessing data from Carto clients

    Parameters
    ----------
    url : str
        URL
    data_set : str
        Dataset ID
    date_field : str
        Name of the column that contains the date
    query : str
        Query that will be perfored for each request

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True)
        Load data for query
    get_count(year=None, where=None)
        Get number of records/rows generated by query
    get_years()
        Get years contained in data set
    """
    
    def __init__(self, url, data_set, date_field=None, query=None):
        '''Create Carto object

        Parameters
        ----------
        url : str
            URL or username for Carto data
        data_set : str
            Dataset ID
        date_field : str
            (Optional) Name of the column that contains the date
        query : str
            (Optional) Additional query that will be added to each request
        '''

        # https://carto.com/developers/sql-api/guides/making-calls/
        # Format of URL is https://{username}.carto.com/api/v2/sql

        # Strip out username to ensure that URL is properly formatted
        username = url
        if username.startswith("https://"):
            username = username.replace("https://", "")

        if ".carto" in username:
            username = username[:username.find(".carto")]

        url_clean = "https://" + username + ".carto.com/api/v2/sql"

        self.url = url_clean
        self.data_set = data_set
        self.date_field = date_field
        self.query = str2json(query)

    
    def isfile(self):
        '''Returns False to indicate that Carto data is not file-based

        Returns
        -------
        False
        '''
        return False
    
    def get_api_url(self):
        return f'{self.url}?q=SELECT * FROM {self.data_set}'


    def get_count(self, year=None, **kwargs):
        '''Get number of records for a data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''

        if self._last_count is not None and self._last_count[0]==year:
            logger.debug("Request matches previous count request. Returning saved count.")
            return self._last_count[1]
        else:
            where = self.__construct_where(year)
            json = self.__request(where=where, return_count=True)
            count = json["rows"][0]["count"]

        self._last_count = (year, count, where)

        return count


    def __request(self, where=None, return_count=False, out_fields="*", out_type="GeoJSON", offset=0, count=None):

        query = "SELECT "
        params = {}
        if return_count:
            query+="count(*)"
        else:
            query+=out_fields
            params["format"] = out_type

        query+=f" FROM {self.data_set}"

        if where != None:
            query+=" WHERE "+ where

        default_where = ''
        for k,v in self.query.items():
            if isinstance(v,str):
                v = f"'{v}'"
            default_where += f" AND {k}={v}"

        if len(default_where):
            if where != None:
                query+=default_where
            else:
                query+=" WHERE"+ default_where[4:]

        if not return_count and count!=0:
            # Order results to ensure data order remains constant if paging
            query+=" ORDER BY cartodb_id"

        query+=f" OFFSET {offset}"

        if count!=None:
            query+=f" LIMIT {count}"

        params["q"] = query

        logger.debug(f"Request data from {self.url}")
        for k,v in params.items():
            logger.debug(f"\t{k} = {v}")

        r = requests.get(self.url, params=params)

        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            if len(e.args)>0:
                if "503 Server Error" in e.args[0]:
                    raise OPD_DataUnavailableError(self.get_api_url(), e.args, _url_error_msg.format(self.get_api_url()))
                else:
                    raise

            else: raise e
        except: raise
        
        return r.json()


    def __construct_where(self, year=None):
        if self.date_field!=None and year!=None:
            start_date, stop_date = _process_date(year, date_field=self.date_field)
            where_query = f"{self.date_field} >= '{start_date}' AND {self.date_field} <= '{stop_date}'"
        else:
            where_query = None

        return where_query

    
    def load(self, year=None, nrows=None, offset=0, *, pbar=True, format_date=True, **kwargs):
        '''Download table to pandas or geopandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the table
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        pbar : bool
            (Optional) If true (default), a progress bar will be displayed
        format_date : bool, optional
            If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
            to be pandas datetimes (or pandas Period in rare cases), by default True
            
        Returns
        -------
        pandas or geopandas DataFrame
            DataFrame containing downloaded
        '''
        
        if self._last_count is not None and self._last_count[0]==year:
            record_count = self._last_count[1]
            where_query = self._last_count[2]
        else:
            where_query = self.__construct_where(year)
            json = self.__request(where=where_query, return_count=True)
            record_count = json["rows"][0]["count"]
            self._last_count = (year, record_count, where_query)

        record_count-=offset
        if record_count<=0:
            return pd.DataFrame()

        batch_size = _default_limit
        nrows = nrows if nrows!=None and record_count>=nrows else record_count
        batch_size = nrows if nrows < batch_size else batch_size
        num_batches = ceil(nrows / batch_size)
            
        pbar = pbar and num_batches>1
        if pbar:
            bar = tqdm(desc=self.url, total=nrows, leave=False)

        # When requesting data as GeoJSON, no type information is returned so request it now
        type_info = self.__request(count=0, out_type="JSON")
            
        features = []
        for batch in range(num_batches):
            bs = batch_size if batch<num_batches-1 else nrows-batch*batch_size

            try:
                data = self.__request(where=where_query, offset=offset+batch*batch_size, count=bs)
                features.extend(data["features"])

                if batch==0 and len(features)>0:
                    date_cols = [key for key, x in type_info["fields"].items() if x["type"]=='date']
                    if len(data["features"]) not in [batch_size, nrows]:
                        num_rows = len(data["features"])
                        raise ValueError(f"Number of rows is {num_rows} but is expected to be max rows to read {batch_size} or total number of rows {nrows}")
            except Exception as e:
                if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                    raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.get_api_url()))
                else:
                    raise
            except:
                raise

            if pbar:
                bar.update(len(data["features"]))

        if pbar:
            bar.close()

        df = pd.DataFrame.from_records([x["properties"] for x in features])
        if format_date:
            for col in date_cols:
                if col in df:
                    logger.debug(f"Column {col} had a data type of date. Converting values to datetime objects.")
                    df[col] = to_datetime(df[col])

        if len(df) > 0:
            has_point_geometry = any("geometry" in x and x["geometry"]!=None for x in features)
            if has_point_geometry:
                if _use_gpd_force is not None:
                    if not _has_gpd and _use_gpd_force:
                        raise ValueError("User cannot force GeoPandas usage when it is not installed")
                    use_gpd = _use_gpd_force
                else:
                    use_gpd = _has_gpd

                if use_gpd:
                    geometry = []
                    for feat in features:
                        if "geometry" not in feat or feat["geometry"]==None or len(feat["geometry"]["coordinates"])<2:
                            geometry.append(None)
                        else:
                            geometry.append(Point(feat["geometry"]["coordinates"][0], feat["geometry"]["coordinates"][1]))

                    logger.debug("Geometry found. Contructing geopandas GeoDataFrame")
                    df = gpd.GeoDataFrame(df, crs=4326, geometry=geometry)
                else:
                    geometry = [feat["geometry"] if "geometry" in feat else None for feat in features]

                    if "geolocation" not in df:
                        logger.debug("Adding geometry column generated from spatial data provided by request.")
                        df["geolocation"] = geometry
                    else:
                        raise KeyError("geolocation already exists in DataFrame")

            return df
        else:
            return pd.DataFrame()


class Socrata(Data_Loader):
    """
    A class for accessing data from Socrata clients

    Parameters
    ----------
    url : str
        URL of data homepage
    data_set : str
        Dataset ID
    date_field : str
        Name of the column that contains the date
    client : sodapy.Socrata
        Socrata client

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True, opt_filter=None, select=None, output_type=None)
        Load data for query
    get_count(year=None, opt_filter=None, where=None)
        Get number of records/rows generated by query
    get_years()
        Get years contained in data set
    """

    def __init__(self, url, data_set, date_field=None, key=default_sodapy_key):
        '''Create Socrata object

        Parameters
        ----------
        url : str
            URL for Socrata data
        data_set : str
            Dataset ID for Socrata data
        date_field : str
            (Optional) Name of the column that contains the date
        key : str
            (Optional) Socrata app token to prevent throttling of the data request
        '''
        self.url = url
        self.data_set = data_set
        self.date_field = date_field
        # Unauthenticated client only works with public data sets. Note 'None'
        # in place of application token, and no username or password:
        self.client = SocrataClient(self.url, key, timeout=90)


    def __construct_where(self, year, opt_filter):
        where = ""
        if self.date_field!=None and year!=None:
            filter_year = False
            assume_date = False
            try:
                # Get metadata to ensure that date is not formatted as text
                meta = self.client.get_metadata(self.data_set)
                column = [x for x in meta['columns'] if x['fieldName']==self.date_field]
                if len(column)>0 and 'dataTypeName' in column[0] and column[0]['dataTypeName']=='text':
                    # The date column is text. It may have some metadata about it's largest value which 
                    # will tell us if it's in YYYY-MM-DD format in which case our filtering will still work.
                    # If not, we can only filter by year with a text search.
                    if not ('cachedContents' in column[0] and 'largest' in column[0]['cachedContents'] and \
                        isinstance(column[0]['cachedContents']['largest'], str) and \
                            re.search(r'^\d{4}\-\d{2}\-\d{2}', column[0]['cachedContents']['largest'])):
                        filter_year = True
            except:
                assume_date = True

            if not assume_date and len(column)==0:
                raise ValueError(f"Date field {self.date_field} not found in dataset")
            if filter_year:
                start_date, stop_date = _process_date(year, date_field=self.date_field, force_year=True)
                where = ''
                for y in range(int(start_date),int(stop_date)+1):
                    # %25 is % wildcard symbol
                    if self.url=='data.bloomington.in.gov' and self.data_set=='gpr2-wqbb':
                        # This dataset has a text date field and contains YYYY/MM/DD and MM/DD/YY formats
                        yy = str(y)[2:]
                        where+=self.date_field + f" LIKE '_/_/{yy}' OR " + \
                               self.date_field + f" LIKE '_/__/{yy}' OR " + \
                               self.date_field + f" LIKE '__/_/{yy}' OR " + \
                               self.date_field + f" LIKE '__/__/{yy}' OR " + \
                               self.date_field + rf" LIKE '{y}%' OR "
                    else:
                        where+=self.date_field + rf" LIKE '%{y}%' OR "
                where = where[:-4]
            else:
                start_date, stop_date = _process_date(year, date_field=self.date_field)
                where = self.date_field + " between '" + start_date + "' and '" + stop_date +"'"

        if opt_filter is not None:
            if not isinstance(opt_filter, list):
                opt_filter = [opt_filter]

            andStr = " AND "
            for filt in opt_filter:
                where += andStr + filt

            if where[0:len(andStr)] == andStr:
                where = where[len(andStr):]

        return where
    

    def isfile(self):
        '''Returns False to indicate that Socrata data is not file-based

        Returns
        -------
        False
        '''
        return False
    
    def get_api_url(self):
        url = self.url[:-1] if self.url.endswith('/') else self.url
        url = url if url.startswith('http') else 'https://'+url
        return f"{url}/resource/{self.data_set}.json"


    def get_count(self, year=None, *,  opt_filter=None, where=None, **kwargs):
        '''Get number of records for a Socrata data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        opt_filter : str
            (Optional) Additional filter to apply to data (beyond any date filter specified by self.date_field and year)
        where: str
            (Optional) where statement for Socrata query. If None, where statement will be constructed from self.date_field, year, and opt_filter
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''

        if where==None:
            where = self.__construct_where(year, opt_filter)

        if self._last_count is not None and self._last_count[0]==(year, opt_filter, where):
            logger.debug("Request matches previous count request. Returning saved count.")
            return self._last_count[1]
        
        logger.debug(f"Request dataset {self.data_set} from {self.url}")
        logger.debug(f"\twhere={where}")
        logger.debug(f"\tselect=count(*)")

        try:
            results = self.client.get(self.data_set, where=where, select="count(*)")
        except (requests.HTTPError, requests.exceptions.ReadTimeout) as e:
            raise OPD_SocrataHTTPError(self.url, self.data_set, *e.args, _url_error_msg.format(self.get_api_url()))
        except Exception as e: 
            if len(e.args)>0 and (e.args[0]=='Unknown response format: text/html' or \
                "Read timed out" in e.args[0]):
                raise OPD_SocrataHTTPError(self.url, self.data_set, *e.args, _url_error_msg.format(self.get_api_url()))
            else:
                raise e  
            
        try:
            num_rows = float(results[0]["count"])
        except:
            num_rows = float(results[0]["count_1"]) # Value used in VT Shootings data

        count = int(num_rows)
        self._last_count = ((year, opt_filter, where),count)

        return count


    def load(self, year=None, nrows=None, offset=0, *, pbar=True, opt_filter=None, select=None, output_type=None, sortby=None, **kwargs):
        '''Download table from Socrata to pandas or geopandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the table
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        pbar : bool
            (Optional) If true (default), a progress bar will be displayed
        opt_filter : str
            (Optional) Additional filter to apply to data (beyond any date filter specified by self.date_field and year)
        select : str
            (Optional) select statement to REST API
        output_type : str
            (Optional) Data type for the output. Allowable values: GeoDataFrame, DataFrame, set, list. Default: GeoDataFrame or DataFrame
        sortby : str
            (Optional) Columns to sort by. Allowable values: None (defaults to id) or "date"
            
        Returns
        -------
        pandas or geopandas DataFrame
            DataFrame containing table
        '''

        N = 1  # Initialize to value > 0 so while loop runs
        start_offset = offset

        where = self.__construct_where(year, opt_filter)
        
        if _use_gpd_force is not None:
            if not _has_gpd and _use_gpd_force:
                raise ValueError("User cannot force GeoPandas usage when it is not installed")
            use_gpd = _use_gpd_force
        else:
            use_gpd = _has_gpd

        record_count = int(self.get_count(where=where))
        record_count-=offset
        if record_count<=0:
            return pd.DataFrame()
        batch_size =  _default_limit
        nrows = nrows if nrows!=None and record_count>=nrows else record_count
        batch_size = nrows if nrows < batch_size else batch_size
        num_batches = ceil(nrows / batch_size)
            
        show_pbar = pbar and num_batches>1 and select==None
        if show_pbar:
            bar = tqdm(desc=f"URL: {self.url}, Dataset: {self.data_set}", total=num_batches, leave=False)

        order = None
        if select == None:
            if self.date_field and isinstance(sortby,str) and sortby=="date":
                order = self.date_field
            else:
                # order guarantees data order remains the same when paging
                # Order by date if available otherwise the data ID. 
                # https://dev.socrata.com/docs/paging.html#2.1
                order = ":id"

        while N > 0:
            logger.debug(f"Request dataset {self.data_set} from {self.url}")
            logger.debug(f"\twhere={where}")
            logger.debug(f"\tselect={select}")
            logger.debug(f"\tlimit={batch_size}")
            logger.debug(f"\toffset={offset}")
            logger.debug(f"\torder={order}")
            try:
                results = self.client.get(self.data_set, where=where,
                    limit=batch_size,offset=offset, select=select, order=order)
            except requests.HTTPError as e:
                raise OPD_SocrataHTTPError(self.url, self.data_set, *e.args, _url_error_msg.format(self.get_api_url()))
            except Exception as e: 
                arg_str = None
                err = e
                while True:
                    if len(err.args):
                        if isinstance(err.args[0],str):
                            arg_str = err.args[0]
                            break
                        elif isinstance(err.args[0],Exception):
                            err = err.args[0]
                        else:
                            break
                    else:
                        break
                if arg_str and (arg_str=='Unknown response format: text/html' or \
                    "Read timed out" in arg_str):
                    raise OPD_SocrataHTTPError(self.url, self.data_set, *e.args, _url_error_msg.format(self.get_api_url()))
                else:
                    raise e

            if use_gpd and output_type==None:
                # Check for geo info
                for r in results:
                    if "geolocation" in r or "geocoded_column" in r:
                        output_type = "GeoDataFrame"
                        break

            if output_type == "set":
                if offset==start_offset:
                    df = set()

                if len(results)>0:
                    filt_key = select.replace("DISTINCT ", "")
                    results = [row[filt_key] for row in results if len(row)>0]
                    results = set(results)
                    df.update(results)

            elif output_type == "list":
                if offset==start_offset:
                    df = list()

                if len(results)>0:
                    [df.append(row[select]) for row in results]

            elif use_gpd and output_type=="GeoDataFrame":
                output_type = "GeoDataFrame"
                # Presumed to be a list of properties that possibly include coordinates
                geojson = {"type" : "FeatureCollection", "features" : []}
                for p in results:
                    feature = {"type" : "Feature", "properties" : p}
                    if "geolocation" in feature["properties"]:
                        geo = feature["properties"].pop("geolocation")
                        if list(geo.keys()) == ["human_address"]:
                            feature["geometry"] = {"type" : "Point", "coordinates" : (nan, nan)}  
                        elif "coordinates" in geo:
                            feature["geometry"] = geo
                        else:
                            feature["geometry"] = {"type" : "Point", "coordinates" : (float(geo["longitude"]), float(geo["latitude"]))}
                    elif "geocoded_column" in feature["properties"]:
                        feature["geometry"] = feature["properties"].pop("geocoded_column")
                    else:
                        feature["geometry"] = {"type" : "Point", "coordinates" : (nan, nan)} 
                    
                    geojson["features"].append(feature)

                if len(results)>0:
                    logger.debug("Geometry found. Contructing geopandas GeoDataFrame")
                    new_gdf = gpd.GeoDataFrame.from_features(geojson, crs=4326)
                        
                    if offset==start_offset:
                        df = new_gdf
                    else:
                        df = pd.concat([df, new_gdf], ignore_index=True)
            else:
                output_type = "DataFrame"
                rows = pd.DataFrame.from_records(results)
                if offset==start_offset:
                    df = pd.DataFrame(rows)
                else:
                    df = pd.concat([df, rows], ignore_index=True)

            N = len(results)
            offset += N

            if show_pbar:
                bar.update()

            if N>=nrows:
                break

        if show_pbar:
            bar.close()

        if isinstance(df, pd.DataFrame) and nrows is not None and len(df)>nrows:
            df = df.head(nrows)
        return df


class Ckan(Data_Loader):
    """
    A class for accessing data from CKAN clients

    Parameters
    ----------
    url : str
        URL
    data_set : str
        Dataset ID
    date_field : str
        Name of the column that contains the date
    query : str
        Query that will be perfored for each request

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True)
        Load data for query
    get_count(year=None, where=None)
        Get number of records/rows generated by query
    get_years()
        Get years contained in data set
    """
    
    def __init__(self, url, data_set, date_field=None, query=None):
        '''Create Ckan object

        Parameters
        ----------
        url : str
            URL or username for Carto data
        data_set : str
            Dataset ID
        date_field : str
            (Optional) Name of the column that contains the date
        query : str
            (Optional) Additional query that will be added to each request
        '''

        # https://docs.ckan.org/en/2.9/maintaining/datastore.html

        if url.startswith("https://"):
            url = url.replace("https://", "")
        if url.endswith('/'):
            url = url[:-1]

        url_clean = "https://" + url + "/api/3/action/datastore_search_sql"

        self.url = url_clean
        self.data_set = data_set
        self.date_field = date_field
        self.query = str2json(query)

    
    def isfile(self):
        '''Returns False to indicate that CKAN data is not file-based

        Returns
        -------
        False
        '''
        return False
    
    def get_api_url(self):
        return f'{self.url}?sql=SELECT * FROM "{self.data_set}"'


    def get_count(self, year=None, opt_filter=None, **kwargs):
        '''Get number of records for a data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''

        if self._last_count is not None and self._last_count[0]==year and self._last_count[1]==opt_filter:
            logger.debug("Request matches previous count request. Returning saved count.")
            return self._last_count[2]
        else:
            where = self.__construct_where(year, opt_filter)
            json = self.__request(where=where, return_count=True)
            count = json['result']['records'][0]['count']

        self._last_count = (year, opt_filter, count, where)

        return count


    def __request(self, where=None, return_count=False, out_fields="*", out_type="json", offset=0, count=None, orderby="_id"):

        if isinstance(out_fields, list):
            out_fields = '"' + '", "'.join(out_fields) + '"'
        elif not out_fields:
            out_fields = '*'

        orderby = self.date_field if orderby=='date' else orderby
        orderby = orderby if orderby else "_id"

        query = "SELECT "
        params = {}
        if return_count:
            query+="COUNT("+ out_fields + ")"
        else:
            query+=out_fields
            params["format"] = out_type

        query+=f' FROM "{self.data_set}"'

        if where != None:
            query+=" WHERE "+ where

        default_where = ''
        for k,v in self.query.items():
            if isinstance(v,str):
                v = f"'{v}'"
            default_where += f" AND {k}={v}"

        if len(default_where):
            if where != None:
                query+=default_where
            else:
                query+=" WHERE"+ default_where[4:]

        if not return_count and count!=0 and not out_fields.startswith("DISTINCT"):
            # Order results to ensure data order remains constant if paging
            query+=' ORDER BY "'+ orderby + '"'

        query+=f" OFFSET {offset}"

        if count!=None:
            query+=f" LIMIT {count}"

        params["sql"] = query

        logger.debug(f"Request data from {self.url}")
        for k,v in params.items():
            logger.debug(f"\t{k} = {v}")

        try:
            r = requests.get(self.url, params=params)
        except requests.exceptions.SSLError as e:
            raise OPD_DataUnavailableError(self.url, e.args, _url_error_msg.format(self.get_api_url()))

        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            if len(e.args)>0:
                if "503 Server Error" in e.args[0]:
                    raise OPD_DataUnavailableError(self.url, e.args, _url_error_msg.format(self.get_api_url()))
                else:
                    raise

            else: raise e
        except: raise
        
        return r.json()


    def __construct_where(self, year=None, opt_filter=None, filter_year=False, sample_data=None):
        if self.date_field!=None and year!=None:
            datetime_format = None
            if not sample_data:
                sample_data = self.__request(count=100)
            
            date_col_info = [x for x in sample_data['result']["fields"] if x["id"]==self.date_field]
            if len(date_col_info)==0:
                raise ValueError(f"Date column {self.date_field} not found")
            filter_year = date_col_info[0]["type"] not in ['timestamp','date']
            if filter_year and date_col_info[0]["type"] == 'text':
                # See if year can be filtered by YYYY-MM-DD 
                dates = [x[self.date_field] for x in sample_data['result']['records']]
                p = re.compile(r'^20\d{2}\-\d{2}\-\d{2}')
                if all([p.search(x) for x in dates]):
                    filter_year = False
                    # Identify time format
                    times = [p.sub('', x) for x in dates]
                    if len(times[0])>0:
                        if times[0][0]==' ':
                            times = [x[1:] for x in times]
                        else:
                            raise ValueError(f"Dates in {self.date_field} are text (not date) values and have unknown format (i.e. {dates[0]})")
                        
                        if all([re.search(r'^\d{2}:\d{2}:\d{2}$',x) for x in times]):
                            datetime_format = r'%Y-%m-%d %H:%M:%S'
                        elif all(m:=[re.search(r'^\d{2}:\d{2}:\d{2}\+(\d{2})$',x) for x in times]):
                            utc_offsets = [x.groups(1)[0] for x in m]
                            if all([x==utc_offsets[0] for x in utc_offsets]):
                                datetime_format = r'%Y-%m-%d %H:%M:%S+' + utc_offsets[0]
                            else:
                                raise ValueError(f"Dates in {self.date_field} are text (not date) values and have varying UTC offset")
                        else:
                            raise ValueError(f"Dates in {self.date_field} are text (not date) values and have unknown format (i.e. {dates[0]})")

            if filter_year:
                start_date, stop_date = _process_date(year, date_field=self.date_field, force_year=True)
                where = '('
                for y in range(int(start_date),int(stop_date)+1):
                    # %25 is % wildcard symbol
                    where+='"' + self.date_field + '"' + rf" LIKE '%{y}%' OR "
                where = where[:-4] + ')'
            else:
                start_date, stop_date = _process_date(year, date_field=self.date_field, datetime_format=datetime_format)
                where = f"""("{self.date_field}" >= '{start_date}' AND "{self.date_field}" <= '{stop_date}')"""
        else:
            where = None

        if opt_filter:
            where = where if where else ""
            if not isinstance(opt_filter, list):
                opt_filter = [opt_filter]

            andStr = " AND "
            for filt in opt_filter:
                where += andStr + filt

            if where[0:len(andStr)] == andStr:
                where = where[len(andStr):]

        return where

    
    def load(self, year=None, nrows=None, offset=0, *, pbar=True, opt_filter=None, select=None, output_type=None, sortby='_id', 
             format_date=True, **kwargs):
        '''Download table to pandas or geopandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the table
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        pbar : bool
            (Optional) If true (default), a progress bar will be displayed
        opt_filter : str
            (Optional) Additional filter to apply to data (beyond any date filter specified by self.date_field and year)
        select : str
            (Optional) select statement to REST API. Default '*' (all fields)
        output_type : str
            (Optional) Data type for the output. Allowable values: 'DataFrame' and 'set'. Default: DataFrame
        sortby : str
            (Optional) Columns to sort by. Default: '_id'
        format_date : bool, optional
            If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
            to be pandas datetimes (or pandas Period in rare cases), by default True
            
        Returns
        -------
        pandas or geopandas DataFrame
            DataFrame containing downloaded
        '''

        data = self.__request(count=100)
        date_cols = [x['id'] for x in data['result']["fields"] if x["type"] in ['timestamp','date']]
        
        if self._last_count is not None and self._last_count[0]==year and self._last_count[1]==opt_filter:
            record_count = self._last_count[2]
            where_query = self._last_count[3]
        else:
            where_query = self.__construct_where(year, opt_filter, sample_data=data)
            json = self.__request(where=where_query, return_count=True, out_fields=select)
            record_count = json['result']['records'][0]['count']
            self._last_count = (year, opt_filter, record_count, where_query)

        record_count-=offset
        if record_count<=0:
            return pd.DataFrame()

        # Default fetch limit per https://docs.ckan.org/en/2.9/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search_sql
        batch_size = 32000
        nrows = nrows if nrows!=None and record_count>=nrows else record_count
        batch_size = nrows if nrows < batch_size else batch_size
        num_batches = ceil(nrows / batch_size)
            
        pbar = pbar and num_batches>1
        if pbar:
            bar = tqdm(desc=self.url, total=nrows, leave=False)

        if select:
            fields = select
        else:
            # CKAN includes a large _full_text and _id columns that are not useful
            # Get info on columns in order to exclude these columns from the returned data
            
            fields = [x['id'] for x in data['result']['fields'] if x['id'] not in ['_id','_full_text']]
            
        features = []
        for batch in range(num_batches):
            bs = batch_size if batch<num_batches-1 else nrows-batch*batch_size

            try:
                data = self.__request(where=where_query, offset=offset+batch*batch_size, count=bs, out_fields=fields, orderby=sortby)
                features.extend(data['result']['records'])

                if batch==0 and len(features)>0:
                    if len(features) not in [batch_size, nrows]:
                        raise ValueError(f"Number of rows is {len(features)} but is expected to be max rows to read {batch_size} or total number of rows {nrows}")
            except Exception as e:
                if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                    raise OPD_TooManyRequestsError(self.url, *e.args, _url_error_msg.format(self.get_api_url()))
                else:
                    raise
            except:
                raise

            if pbar:
                bar.update(len(data['result']['records']))

        if pbar:
            bar.close()

        df = pd.DataFrame(features)
        if format_date:
            for col in date_cols:
                if col in df:
                    logger.debug(f"Column {col} had a data type of date. Converting values to datetime objects.")
                    df[col] = to_datetime(df[col])

        if len(df) > 0:
            if output_type=='set':
                return df.iloc[:,0].unique()
            else:
                return df
        else:
            return pd.DataFrame()


class Html(Data_Loader):
    """
    A class for accessing data from HTML download URLs

    Parameters
    ----------
    url : str
        URL
    date_field : str
        Name of the column that contains the date
    agency_field : str
        Name of column that contains agency names

    Methods
    -------
    load(year=None, nrows=None, offset=0, pbar=True, agency=None)
        Load data for query
    get_count(year=None, agency=None, force=False)
        Get number of records/rows generated by query
    get_years(force=False)
        Get years contained in data set
    """

    def __init__(self, url, date_field=None, agency_field=None):
        '''Create Html object

        Parameters
        ----------
        url : str
            URL for HTML data
        date_field : str
            (Optional) Name of the column that contains the date
        agency_field : str
                (Optional) Name of the column that contains the agency name (i.e. name of the police departments)
        '''
        
        self.url = url
        self.date_field = date_field
        self.agency_field = agency_field


    def isfile(self):
        '''Returns True to indicate that Html data is file-based

        Returns
        -------
        True
        '''
        return True


    def get_count(self, year=None, *,  agency=None, force=False, **kwargs):
        '''Get number of records for a Html data request
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
        agency : str
            (Optional) Name of agency to filter for.
        force : bool
            (Optional) get_count for HLT file will only run if force=true. In many use cases, it will be more efficient to load the file and manually get the count.
            
        Returns
        -------
        int
            Record count or number of rows in data request
        '''

        logger.debug(f"Calculating row count for {self.url}")
        if self._last_count is not None and self._last_count[0] == (self.url, year, agency):
            logger.debug("Request matches previous count request. Returning saved count.")
            return self._last_count[1]
        
        if force:
            count = len(self.load(year=year, agency=agency))
        else:
            raise ValueError("Extracting the number of records for a single year of a HTML file requires reading the whole file in. In most cases, "+
                "running load() with a year argument to load in the data and manually finding the record count will be more "
                "efficient. If running get_count with a year argument is still desired, set force=True")
        
        self._last_count = ((self.url, year, agency), count)
        return count


    def load(self, year=None, nrows=None, offset=0, *, pbar=True, agency=None, format_date=True, **kwargs):
        '''Download HTML file to pandas DataFrame
        
        Parameters
        ----------
        year : int, list
            (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested. None value returns data for all years.
        nrows : int
            (Optional) Only returns the first nrows rows of the HTML table
        offset - int
            (Optional) Number of records to offset from first record. Default is 0 to return records starting from the first.
        pbar : bool
            (Optional) If true (default), a progress bar will be displayed
        agency : str
            (Optional) Name of the agency to filter for. None value returns data for all agencies.
        format_date : bool, optional
            If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
            to be pandas datetimes (or pandas Period in rare cases), by default True
            
        Returns
        -------
        pandas DataFrame
            DataFrame containing table imported from the HTML table
        '''

        if isinstance(nrows, float):
            nrows = int(nrows)
        
        logger.debug(f"Loading file from {self.url}")

        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'}
        table = pd.read_html(self.url, storage_options=header)
                
        if len(table)>1:
            raise ValueError(f"More than 1 table found at {self.url}")

        
        table = table[0]

        if self.url=='https://www.openpolicedata.com/CedarLakeIN/Citations/2018Citations.php' and len(table)>994:
            # There is a known issue with 1 of the rows
            vals = table.loc[994,table.loc[994].notnull()].tolist()
            if len(vals)<len(table.columns):
                newvals = []
                for v in vals:
                    v = v.split(',')
                    v = [x.strip().strip('"').strip('\\') for x in v]
                    newvals.extend(v)

                if len(newvals)==len(table.columns):
                    table.loc[994,:] = newvals

        
        table = filter_dataframe(table, date_field=self.date_field, year_filter=year, 
            agency_field=self.agency_field, agency=agency, format_date=format_date)

        if offset>0:
            rows_limit = offset+nrows if nrows is not None and offset+nrows<len(table) else len(table)
            logger.debug(f"Extracting {rows_limit} rows starting at {offset}")
            table = table.iloc[offset:rows_limit].reset_index(drop=True)
        if nrows is not None and len(table)>nrows:
            logger.debug(f"Extracting the first {nrows} rows")
            table = table.head(nrows)

        return table

    def get_years(self, *, force=False, **kwargs):
        '''Get years contained in data set
        
        Parameters
        ----------
        force : bool
            (Optional) If false, an exception will be thrown. It may be more efficient to load the table and extract years manually
            
        Returns
        -------
        list
            list containing years in data set
        '''

        if not force:
            raise ValueError("Extracting the years of an HTML table requires reading the whole file in. In most cases, "+
                "running load() with no arguments to load in the whole HTML table and manually finding the years will be more "
                "efficient. If running get_years is still desired, set force=True")
        else:
            if self.date_field==None:
                raise ValueError("No date field provided to access year information")
            df = self.load()
            if self.date_field.lower()=="year":
                years = df[self.date_field].unique()
            else:
                date_col = to_datetime(df[self.date_field])
                years = list(date_col.dt.year.dropna().unique())
            years.sort()
            return [int(x) for x in years]


def _check_year(year):
    return isinstance(year, int) or (isinstance(year, str) and len(year)==4 and year.isdigit())


def _process_date(date, date_field=None, force_year=False, datetime_format=None, is_date_string=False):
    if not isinstance(date, list):
        date = [date, date]

    if len(date)!=2:
        raise ValueError("date should be a list of length 2: [startYear, stopYear]")

    if date[0] > date[1]:
        raise ValueError('date[0] needs to be smaller than or equal to date[1]')
    
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


def str2json(json_str):
    if pd.isnull(json_str):
        return {}
    # Remove any curly quotes
    json_str = json_str.replace('“','"').replace('”','"')
    return json.loads(json_str)


# https://stackoverflow.com/questions/73093656/progress-in-bytes-when-reading-csv-from-url-with-pandas
class TqdmReader:
    # Older versions of pandas check if reader has these properties even though they are not used
    write = []
    __iter__ = []
    def __init__(self, resp, pbar=True, nrows=None):
        total_size = int(resp.headers.get("Content-Length", 0))

        self.rows_read = 0
        if nrows != None:
            self.nrows = nrows
        else:
            self.nrows = float("inf")
        self.resp = resp
        self.pbar = pbar
        if self.pbar:
            self.bar = tqdm(
                desc=resp.url,
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                leave=False
            )

        self.reader = self.read_from_stream()

    def read_from_stream(self):
        for line in self.resp.iter_lines():
            line += b"\n"
            if self.pbar:
                self.bar.update(len(line))
            yield line

    def read(self, n=0):
        try:
            if self.rows_read >= self.nrows:
                # Number of rows read is greater than user-requested limit
                return ""
            self.rows_read += 1
            return next(self.reader)
        except StopIteration:
            if self.pbar:
                self.bar.update(self.bar.total - self.bar.n)
            return ""