import numbers
import os
from datetime import date
import pandas as pd
from numpy import nan
import requests
import urllib
from sodapy import Socrata
from pyproj.exceptions import CRSError
from pyproj import CRS
import warnings
from arcgis.features import FeatureLayerCollection
from arcgis.geometry._types import Point
from time import sleep
from tqdm import tqdm
from math import ceil
import re

try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

try:
    from .exceptions import OPD_TooManyRequestsError, OPD_DataUnavailableError, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError
except:
    from exceptions import OPD_TooManyRequestsError, OPD_DataUnavailableError, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError

sleep_time = 0.1

# Global parameter for testing both with and without GeoPandas in testing
_use_gpd_force = None

_default_limit = 100000
_url_error_msg = "There is likely an issue with the website. Open the URL {} with a web browser to confirm. " + \
                    "See a list of known site outages at https://github.com/openpolicedata/opd-data/blob/main/outages.csv"

_last_arcgis_url = None
_last_arcgis_date_format = None

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: http://dev.socrata.com/docs/app-tokens.html
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
# Windows: https://www.wikihow.com/Create-an-Environment-Variable-in-Windows-10
default_sodapy_key = os.environ.get("SODAPY_API_KEY")

def load_csv(url, date_field=None, year_filter=None, agency_field=None, agency=None, limit=None, pbar=True):
    '''Download CSV file to pandas DataFrame
    
    Parameters
    ----------
    url : str
        Download URL for CSV
    date_field : str
        (Optional) Name of the column that contains the date
    year_filter : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested. None value returns data for all years.
    agency_field : str
        (Optional) Name of the column that contains the agency name (i.e. name of the police departments)
    agency : str
        (Optional) Name of the agency to filter for. None value returns data for all agencies.
    limit : int
        (Optional) Only returns the first limit rows of the CSV
    pbar : bool
        (Optional) If true (default), a progress bar will be displayed
        
    Returns
    -------
    pandas DataFrame
        DataFrame containing table imported from CSV
    '''
    
    if ".zip" in url or not pbar:
        with warnings.catch_warnings():
            # Perhaps use requests iter_content/iter_lines as below to read large CSVs so progress can be shown
            warnings.simplefilter("ignore", category=pd.errors.DtypeWarning)
            try:
                table = pd.read_csv(url, encoding_errors='surrogateescape')
            except urllib.error.HTTPError as e:
                raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(url))
            except Exception as e:
                raise e
    else:
        r = requests.head(url)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise OPD_DataUnavailableError(*e.args, _url_error_msg.format(url))
        except Exception as e:
            raise e
        with requests.get(url, params=None, stream=True) as resp:
            try:
                table = pd.read_csv(TqdmReader(resp), nrows=limit, encoding_errors='surrogateescape')
            except Exception as e:
                raise e

    table = filter_dataframe(table, date_field=date_field, year_filter=year_filter, 
        agency_field=agency_field, agency=agency)

    return table


def load_arcgis(url, date_field=None, year=None, limit=None, pbar=True):
    '''Download table from ArcGIS to pandas or geopandas DataFrame
    
    Parameters
    ----------
    url : str
        ArcGIS GeoService URL
    date_field : str
        (Optional) Name of the column that contains the date
    year : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
    limit : int
        (Optional) Only returns the first limit rows of the table
    pbar : bool
        (Optional) If true (default), a progress bar will be displayed
        
    Returns
    -------
    pandas or geopandas DataFrame
        DataFrame containing table imported from ArcGIS
    '''
    
    # Table vs. Layer: https://developers.arcgis.com/rest/services-reference/enterprise/layer-feature-service-.htm
    # The layer resource represents a single feature layer or a nonspatial table in a feature service. 
    # A feature layer is a table or view with at least one spatial column.
    # For tables, it provides basic information about the table such as its ID, name, fields, types, and templates. 
    # For feature layers, in addition to the table information, it provides information such as its geometry type, min and max scales, and spatial reference.

    p = re.search(r"(MapServer|FeatureServer)/\d+", url)
    url = url[:p.span()[1]]

    global _last_arcgis_url, _last_arcgis_date_format
    if url != _last_arcgis_url:
        _last_arcgis_date_format = None
        _last_arcgis_url = url

    last_slash = url.rindex("/")
    layer_num = url[last_slash+1:]
    base_url = url[:last_slash]
    
    user_limit = limit != None
    if not user_limit:
        limit = _default_limit
    else:
        total = limit

    # Get metadata
    r = requests.get(base_url + "/" + layer_num + "?f=pjson")

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        if len(e.args)>0:
            if "503 Server Error" in e.args[0]:
                raise OPD_DataUnavailableError(base_url, f"Layer # = {layer_num}", e.args)

        else: raise e
    except e: raise e
    
    meta = r.json()
    if "maxRecordCount" in meta and \
        (not user_limit or (user_limit and limit > meta["maxRecordCount"])):
        limit = meta["maxRecordCount"]
    
    # https://developers.arcgis.com/python/
    try:
        layer_collection = FeatureLayerCollection(base_url)
    except Exception as e:
        if len(e.args)>0:
            if "Error Code: 500" in e.args[0]:
                raise OPD_DataUnavailableError(base_url, f"Layer # = {layer_num}", e.args)
            elif "A general error occurred: 'authInfo'" in e.args[0]:
                raise OPD_arcgisAuthInfoError(base_url, f"Layer # = {layer_num}", e.args)
        else: raise e
    except e: raise e

    is_table = True
    active_layer = None
    for layer in layer_collection.layers:
        layer_url = layer.url
        if layer_url[-1] == "/":
            layer_url = layer_url[:-1]
        if layer_num == layer_url[last_slash+1:]:
            active_layer = layer
            is_table = False
            break

    if is_table:
        for layer in layer_collection.tables:
            layer_url = layer.url
            if layer_url[-1] == "/":
                layer_url = layer_url[:-1]
            if layer_num == layer_url[last_slash+1:]:
                active_layer = layer
                break

    if active_layer == None:
        raise ValueError("Unable to find layer")
    
    where_query = ""
    if date_field!=None and year!=None:
        where_query, record_count = _build_arcgis_where_query(base_url, layer_num, active_layer, date_field, year, _last_arcgis_date_format)
    else:
        where_query = '1=1'
        try:
            record_count = active_layer.query(where=where_query, return_count_only=True)
        except Exception as e:
            if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                raise OPD_TooManyRequestsError(base_url, f"Layer # = {layer_num}", *e.args, _url_error_msg.format(url))
            else:
                raise
        except:
            raise

    if record_count==0:
        return None
   
    if user_limit:
        num_batches = ceil(total / limit)
    else:
        num_batches = ceil(record_count / limit)
        total = record_count
        
    df = []
    pbar = pbar and num_batches>1
    if pbar:
        bar = tqdm(desc=url, total=total, leave=False) 
        
    for batch in range(num_batches):
        try:
            if batch==0:
                layer_query_result = active_layer.query(where=where_query, result_offset=batch*limit, result_record_count=limit, return_all_records=False)
                df.append(layer_query_result.sdf)
                if len(df[0]) not in [limit, total]:
                    num_rows = len(df[0])
                    raise ValueError(f"Number of rows is {num_rows} but is expected to be max rows to read {limit} or total number of rows {total}")
            else:
                df.append(active_layer.query(where=where_query, result_offset=batch*limit, result_record_count=limit, return_all_records=False, as_df=True))
        except Exception as e:
            if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                raise OPD_TooManyRequestsError(base_url, f"Layer # = {layer_num}", *e.args, _url_error_msg.format(url))
            else:
                raise
        except:
            raise

        if pbar:
            bar.update(len(df[-1]))

    if pbar:
        bar.close()

    df = pd.concat(df, ignore_index=True)

    if len(df) > 0:
        if is_table:
            if "SHAPE" in df:
                raise ValueError("Tables are not expected to include geographic data")
            return df
        else:
            if _use_gpd_force is not None:
                if not _has_gpd and _use_gpd_force:
                    raise ValueError("User cannot force GeoPandas usage when it is not installed")
                use_gpd = _use_gpd_force
            else:
                use_gpd = _has_gpd

            if use_gpd:
                def fix_nans(pt):
                    if type(pt) == Point and pt.x=="NaN":
                        pt.x = nan
                        pt.y = nan

                    return pt
                geometry = df.pop("SHAPE").apply(fix_nans)
                try:
                    df = gpd.GeoDataFrame(df, crs=layer_query_result.spatial_reference['wkid'], geometry=geometry)
                except CRSError:
                    # Method recommended by pyproj to deal with CRSError for wkid = 102685
                    crs = CRS.from_authority("ESRI", layer_query_result.spatial_reference['wkid'])
                    df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
                except Exception as e:
                    raise e

            return df
    else:
        return None


def load_socrata(url, data_set, date_field=None, year=None, opt_filter=None, select=None, output_type=None, 
                 limit=None, key=default_sodapy_key, pbar=True):
    '''Download table from Socrata to pandas or geopandas DataFrame
    
    Parameters
    ----------
    url : str
        URL for Socrata data
    data_set : str
        Dataset ID for Socrata data
    date_field : str
        (Optional) Name of the column that contains the date
    year : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
    opt_filter : str
        (Optional) Additional filter to apply to data (beyond any date filter specified by date_field and year)
    select : str
        (Optional) select statement to REST API
    output_type : str
        (Optional) Data type for the output. Default: pandas or geopandas DataFrame
    limit : int
        (Optional) Only returns the first limit rows of the table
    key : str
        (Optional) Socrata app token to prevent throttling of the data request
    pbar : bool
        (Optional) If true (default), a progress bar will be displayed
        
    Returns
    -------
    pandas or geopandas DataFrame
        DataFrame containing table
    '''

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(url, key, timeout=60)

    user_limit = limit != None
    if not user_limit:
        limit = _default_limit

    N = 1  # Initialize to value > 0 so while loop runs
    offset = 0

    where = ""
    if date_field!=None and year!=None:
        start_date, stop_date = _process_date(year, date_field=date_field)
        where = date_field + " between '" + start_date + "' and '" + stop_date +"'"

    if opt_filter is not None:
        if not isinstance(opt_filter, list):
            opt_filter = [opt_filter]

        andStr = " AND "
        for filt in opt_filter:
            where += andStr + filt

        if where[0:len(andStr)] == andStr:
            where = where[len(andStr):]

    if _use_gpd_force is not None:
        if not _has_gpd and _use_gpd_force:
            raise ValueError("User cannot force GeoPandas usage when it is not installed")
        use_gpd = _use_gpd_force
    else:
        use_gpd = _has_gpd
         
    show_pbar = pbar and not user_limit and select==None
    if show_pbar:
        results = client.get(data_set, where=where, select="count(*)")
        try:
            num_rows = float(results[0]["count"])
        except:
            num_rows = float(results[0]["count_1"]) # Value used in VT Shootings data
        total = ceil(num_rows / limit)
        if total > 1:
            bar = tqdm(desc=f"URL: {url}, Dataset: {data_set}", total=total, leave=False)
        else:
            show_pbar = False

    order = None
    if select == None:
        # order guarantees data order remains the same when paging
        # Order by date if available otherwise the data ID. 
        # https://dev.socrata.com/docs/paging.html#2.1
        order = ":id" if date_field==None else date_field

    while N > 0:
        try:
            results = client.get(data_set, where=where,
                limit=limit,offset=offset, select=select, order=order)
        except requests.HTTPError as e:
            raise OPD_SocrataHTTPError(url, data_set, *e.args, _url_error_msg.format(url))
        except Exception as e: 
            if len(e.args)>0 and e.args[0]=='Unknown response format: text/html':
                raise OPD_SocrataHTTPError(url, data_set, *e.args, _url_error_msg.format(url))
            else:
                raise e

        if use_gpd and output_type==None:
            # Check for geo info
            for r in results:
                if "geolocation" in r or "geocoded_column" in r:
                    output_type = "GeoDataFrame"
                    break

        if output_type == "set":
            if offset==0:
                df = set()

            if len(results)>0:
                filt_key = select.replace("DISTINCT ", "")
                results = [row[filt_key] for row in results if len(row)>0]
                results = set(results)
                df.update(results)

        elif output_type == "list":
            if offset==0:
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
                new_gdf = gpd.GeoDataFrame.from_features(geojson, crs=4326)
                    
                if offset==0:
                    df = new_gdf
                else:
                    df = pd.concat([df, new_gdf], ignore_index=True)
        else:
            output_type = "DataFrame"
            rows = pd.DataFrame.from_records(results)
            if offset==0:
                df = pd.DataFrame(rows)
            else:
                df = pd.concat([df, rows], ignore_index=True)

        N = len(results)
        offset += N

        if user_limit:
            break
        if show_pbar:
            bar.update()

    if show_pbar:
        bar.close()
    return df


def _process_date(date, date_field=None):
    if not isinstance(date, list):
        date = [date, date]

    if len(date) !=2:
        raise ValueError("date should be a list of length 2: [startYear, stopYear]")

    if date[0] > date[1]:
        raise ValueError('date[0] needs to be smaller than or equal to date[1]')

    if type(date[0]) == str:
        # This should already be in date format
        start_date = date[0]
    elif date_field != None and date_field.lower() == "year":
        # Assuming this as actually a string or numeric field for the year rather than a datestamp
        start_date = str(date[0])
    else:
        start_date = str(date[0]) + "-01-01"

    if type(date[1]) == str:
        # This should already be in date format
        stop_date = date[1]
    elif date_field != None and date_field.lower() == "year":
        # Assuming this as actually a string or numeric field for the year rather than a datestamp
        stop_date = str(date[1])
    else:
            stop_date  = str(date[1]) + "-12-31T23:59:59.999"

    return start_date, stop_date


def filter_dataframe(df, date_field=None, year_filter=None, agency_field=None, agency=None):
    '''Load CSV file to pandas DataFrame
    
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
    '''
    
    if year_filter != None and date_field != None:
        df = df[df[date_field].dt.year == year_filter]

    if agency != None and agency_field != None:
        df = df.query(agency_field + " = '" + agency + "'")

    return df


def get_years_argis(url, date_field):
    '''Returns the years of the data contained in an ArcGIS table
    
    Parameters
    ----------
    url : str
        ArcGIS GeoService URL
    date_field : str
        Name of the column that contains the date
        
    Returns
    -------
    list
        List of years contained in the table
    '''
    
    return _get_years("arcgis", url, date_field=date_field)


def get_years_socrata(url, data_set, date_field):
    '''Returns the years of the data contained in a Socrata table
    
    Parameters
    ----------
    url : str
        URL of Socrata table
    date_field : str
        Name of the column that contains the date
        
    Returns
    -------
    list
        List of years contained in the table
    '''
    
    return _get_years("socrata", url, date_field=date_field, data_set=data_set)


def _get_years(data_type, url, date_field, data_set=None):
    year = date.today().year
    data_type = data_type.lower()

    oldest_recent = 20
    max_misses_gap = 10
    max_misses = oldest_recent
    misses = 0
    years = []
    while misses < max_misses:
        if data_type == "arcgis":
            df = load_arcgis(url, date_field=date_field, year=year, limit=1)
        elif data_type == "socrata":
            df = load_socrata(url, data_set, date_field=date_field, year=year, limit=1)
        else:
            raise ValueError("Unknown data type")

        if not hasattr(df, '__len__') or len(df)==0:  # If doesn't have len attribute, it is None
            misses+=1
        else:
            misses = 0
            max_misses = max_misses_gap
            years.append(year)

        sleep(sleep_time)

        year-=1

    return years

# https://stackoverflow.com/questions/73093656/progress-in-bytes-when-reading-csv-from-url-with-pandas
class TqdmReader:
    # Older versions of pandas check if reader has these properties even though they are not used
    write = []
    __iter__ = []
    def __init__(self, resp, limit=None):
        total_size = int(resp.headers.get("Content-Length", 0))

        self.rows_read = 0
        if limit != None:
            self.limit = limit
        else:
            self.limit = float("inf")
        self.resp = resp
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
            self.bar.update(len(line))
            yield line

    def read(self, n=0):
        try:
            if self.rows_read >= self.limit:
                # Number of rows read is greater than user-requested limit
                return ""
            self.rows_read += 1
            return next(self.reader)
        except StopIteration:
            self.bar.update(self.bar.total - self.bar.n)
            return ""

def _build_arcgis_where_query(base_url, layer_num, active_layer, date_field, year, date_format):
    global _last_arcgis_date_format
    record_count = 0
    where_query = ""
    if date_format==0 or date_format==None:
        start_date, stop_date = _process_date(year)
        
        where_query = f"{date_field} >= '{start_date}' AND  {date_field} < '{stop_date}'"
    
        try:
            record_count = active_layer.query(where=where_query, return_count_only=True)
        except Exception as e:
            if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                raise OPD_TooManyRequestsError(base_url, f"Layer # = {layer_num}", *e.args, _url_error_msg.format(url))
            elif len(e.args)>0 and "Unable to complete operation.\n(Error Code: 400)" in e.args[0]:
                # This query throws an error for this dataset. Try another one below
                pass
            else:
                raise
        except:
            raise

        if record_count>0 or date_format==0:
            _last_arcgis_date_format = 0
            return where_query, record_count

    where_formats = [
        "{} LIKE '%[0-9][0-9]/[0-9][0-9]/{}%'",   # mm/dd/yyyy
        "{} LIKE '{}/[0-9][0-9]'",                # yyyy/mm
        "{} = {}",                # yyyy/mm
    ]
    # Make year iterable
    year = [year] if isinstance(year, numbers.Number) else year

    for format in where_formats:
        if date_format==format or date_format==None:
            where_query = format.format(date_field, year[0])
            for x in year[1:]:
                where_query = f"{where_query} or " + format.format(date_field, x)

            try:
                record_count = active_layer.query(where=where_query, return_count_only=True)
            except Exception as e:
                if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                    raise OPD_TooManyRequestsError(base_url, f"Layer # = {layer_num}", *e.args, _url_error_msg.format(url))
                elif len(e.args)>0 and "Unable to complete operation.\n(Error Code: 400)" in e.args[0]:
                    # This query throws an error for this dataset. Try another one below
                    pass
                else:
                    raise
            except:
                pass

            if record_count>0 or date_format==format:
                _last_arcgis_date_format = format
                return where_query, record_count

    return where_query, record_count

        
if __name__ == "__main__":
    import time
    _default_limit = 10000
    start_time = time.time()
    url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/13/"
    # url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
    date_field = 'YR'
    years = _get_years("ArcGIS", url, date_field)
    load_arcgis(url, date_field, [2020,2021])
    print(f"Completed in {time.time()-start_time} seconds")
