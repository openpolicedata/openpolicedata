import os
from datetime import date
import geopandas as gpd
import pandas as pd
from numpy import nan
from sodapy import Socrata
import requests
import contextlib
import urllib
import json
from pyproj.exceptions import CRSError
from pyproj import CRS
import warnings

from arcgis.features import FeatureLayerCollection

if __name__ == '__main__':
    from exceptions import OPD_TooManyRequestsError, OPD_DataUnavailableError
else:
    from .exceptions import OPD_TooManyRequestsError, OPD_DataUnavailableError

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: http://dev.socrata.com/docs/app-tokens.html
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
# Windows: https://www.wikihow.com/Create-an-Environment-Variable-in-Windows-10
default_sodapy_key = os.environ.get("SODAPY_API_KEY")

def load_csv(url, date_field=None, year_filter=None, jurisdiction_field=None, jurisdiction_filter=None, limit=None):
    '''Download CSV file to pandas DataFrame
    
    Parameters
    ----------
    url : str
        Download URL for CSV
    date_field : str
        (Optional) Name of the column that contains the date
    year_filter : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested. None value returns data for all years.
    jurisdiction_field : str
        (Optional) Name of the column that contains the jurisidiction name (i.e. name of the police departments)
    jurisdiction_filter : str
        (Optional) Name of the jurisdiction to filter for. None value returns data for all jurisdictions.
    limit : int
        (Optional) Only returns the first limit rows of the CSV
        
    Returns
    -------
    pandas DataFrame
        DataFrame containing table imported from CSV
    '''
    
    if limit==None or ".zip" in url:
        with warnings.catch_warnings():
            # Perhaps use requests iter_content/iter_lines as below to read large CSVs so progress can be shown
            warnings.simplefilter("ignore", category=pd.errors.DtypeWarning)
            table = pd.read_csv(url)
    else:
        table = pd.DataFrame()
        with contextlib.closing(urllib.request.urlopen(url=url)) as rd:
            for df in pd.read_csv(rd, chunksize=1024):
                table = pd.concat([table, df], ignore_index=True)
                if len(table) > limit:
                    break

    if limit!=None and len(table) > limit:
        table = table.head(limit)


    table = filter_dataframe(table, date_field=date_field, year_filter=year_filter, 
        jurisdiction_field=jurisdiction_field, jurisdiction_filter=jurisdiction_filter)

    return table


def load_geojson(url, date_field=None, year_filter=None, jurisdiction_field=None, jurisdiction_filter=None):
    '''Download GeoJSON file to geopandas DataFrame
    
    Parameters
    ----------
    url : str
        Download URL for GeoJSON file
    date_field : str
        (Optional) Name of the column that contains the date
    year_filter : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
    jurisdiction_field : str
        (Optional) Name of the column that contains the jurisidiction name (i.e. name of the police departments)
    jurisdiction_filter : str
        (Optional) Name of the jurisdiction to filter for.  None value returns data for all jurisdictions.
        
    Returns
    -------
    geopandas DataFrame
        DataFrame containing table imported from GeoJSON file
    '''
    
    try:
        response = requests.get(url)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except requests.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.6
        Exception()
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6
        Exception()

    json_data = response.json()

    df = gpd.GeoDataFrame.from_features(json_data, crs=json_data["crs"]["properties"]["name"])

    if date_field != None:
        df = df.astype({date_field: 'datetime64[ns]'})

    df = filter_dataframe(df, date_field=date_field, year_filter=year_filter, 
        jurisdiction_field=jurisdiction_field, jurisdiction_filter=jurisdiction_filter)

    return df

def load_arcgis(url, date_field=None, year=None, limit=None):
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

    if url[-1] == "/":
        url = url[0:-1]
    last_slash = url.rindex("/")
    layer_num = url[last_slash+1:]
    url = url[:last_slash]
    # Get layer/table #
    # Shorten URL
    
    # https://developers.arcgis.com/python/
    try:
        layer_collection = FeatureLayerCollection(url)
    except Exception as e:
        if len(e.args)>0 and "Error Code: 500" in e.args[0]:
            raise OPD_DataUnavailableError(e.args[0])
        else:
            raise
    except:
        raise

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
        start_date, stop_date = _process_date(year, inclusive=False)
        
        where_query = f"{date_field} >= '{start_date}' AND  {date_field} < '{stop_date}'"
        try:
            layer_query_result = active_layer.query(where=where_query, return_all_records=(limit == None), result_record_count=limit)
        except Exception as e:
            if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                raise OPD_TooManyRequestsError(e.args[0])
            else:
                raise
        except:
            raise
    else:
        try:
            layer_query_result = active_layer.query(return_all_records=(limit == None), result_record_count=limit)
        except Exception as e:
            if len(e.args)>0 and "Error Code: 429" in e.args[0]:
                raise OPD_TooManyRequestsError(e.args[0])
            else:
                raise
        except:
            raise

    if len(layer_query_result) > 0:
        if is_table:
            if len(layer_query_result.features) > 0 and layer_query_result.features[0].geometry != None:
                raise ValueError("Tables are not expected to include geographic data")
            return layer_query_result.sdf
        else:
            try:
                json_data = layer_query_result.to_geojson
            except:
                for k in range(len(layer_query_result.features)):
                    if layer_query_result.features[k].geometry == None:
                        # Put in dummy data
                        layer_query_result.features[k].geometry = {"x" : nan, "y" : nan}

                json_data = layer_query_result.to_geojson

            json_data = json.loads(json_data)
            
            try:
                geo_data_frame = gpd.GeoDataFrame.from_features(json_data, crs=layer_query_result.spatial_reference['wkid'])
            except CRSError:
                # Method recommended by pyproj to deal with CRSError for wkid = 102685
                crs = CRS.from_authority("ESRI", layer_query_result.spatial_reference['wkid'])
                geo_data_frame = gpd.GeoDataFrame.from_features(json_data, crs=crs)

            if date_field is not None:
                date_field_metadata=[x for x in layer_query_result.fields if x['name']==date_field]
                if len(date_field_metadata) != 1:
                    raise ValueError(f"Unable to find a single date field named {date_field}. Found {len(date_field_metadata)} instances.")

                if date_field_metadata[0]['type'] == 'esriFieldTypeDate':
                    geo_data_frame = geo_data_frame.astype({date_field: 'datetime64[ms]'})
                else:
                    raise ValueError(f"Unsupported data type {date_field_metadata[0]['type']} for field {date_field}.")

            return geo_data_frame
    else:
        return None


def load_socrata(url, data_set, date_field=None, year=None, opt_filter=None, select=None, output_type=None, 
                 limit=None, key=default_sodapy_key):
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
        
    Returns
    -------
    pandas or geopandas DataFrame
        DataFrame containing table
    '''

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(url, key)

    userLimit = limit != None
    if not userLimit:
        limit = client.DEFAULT_LIMIT

    N = 1  # Initialize to value > 0 so while loop runs
    offset = 0

    where = ""
    if date_field!=None and year!=None:
        start_date, stop_date = _process_date(year,inclusive=True)
        where = date_field + " between '" + start_date + "' and '" + stop_date +"'"

    if opt_filter is not None:
        if not isinstance(opt_filter, list):
            opt_filter = [opt_filter]

        andStr = " AND "
        for filt in opt_filter:
            where += andStr + filt

        if where[0:len(andStr)] == andStr:
            where = where[len(andStr):]

    while N > 0:
        results = client.get(data_set, where=where,
            limit=limit,offset=offset, select=select)

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

        elif output_type=="GeoDataFrame" or (output_type==None and len(results)>0 and "geolocation" in results[0]):
            output_type = "GeoDataFrame"
            # Presumed to be a list of properties that possibly include coordinates
            geojson = {"type" : "FeatureCollection", "features" : []}
            for p in results:
                feature = {"type" : "Feature", "properties" : p}
                geo = feature["properties"].pop("geolocation")
                feature["geometry"] = {"type" : "Point", "coordinates" : (float(geo["longitude"]), float(geo["latitude"]))}  
                geojson["features"].append(feature)

            if len(results)>0:
                new_gdf = gpd.GeoDataFrame.from_features(geojson, crs=4326)
                    
                if offset==0:
                    df = new_gdf
                else:
                    df = df.append(new_gdf)
        else:
            output_type = "DataFrame"
            rows = pd.DataFrame.from_records(results)
            if offset==0:
                df = pd.DataFrame(rows)
            else:
                df = df.append(rows)

        N = len(results)
        offset += N

        if userLimit:
            break

    return df


def _process_date(date, inclusive):
    if not isinstance(date, list):
        date = [date, date]

    if len(date) !=2:
        raise ValueError("date should be a list of length 2: [startYear, stopYear]")

    if date[0] > date[1]:
        raise ValueError('date[0] needs to be smaller than or equal to date[1]')

    if type(date[0]) == str:
        # This should already be in date format
        start_date = date[0]
    else:
        start_date = str(date[0]) + "-01-01"

    if type(date[1]) == str:
        # This should already be in date format
        stop_date = date[1]
    else:
        if inclusive:
            stop_date  = str(date[1]) + "-12-31T23:59:59.999"
        else:
            stop_date  = str(date[1]+1) + "-01-01"

    return start_date, stop_date


def filter_dataframe(df, date_field=None, year_filter=None, jurisdiction_field=None, jurisdiction_filter=None):
    '''Load CSV file to pandas DataFrame
    
    Parameters
    ----------
    df : pandas or geopandas dataframe
        Dataframe containing the data
    date_field : str
        (Optional) Name of the column that contains the date
    year_filter : int, list
        (Optional) Either the year or the year range [first_year, last_year] for the data that is being requested.  None value returns data for all years.
    jurisdiction_field : str
        (Optional) Name of the column that contains the jurisidiction name (i.e. name of the police departments)
    jurisdiction_filter : str
        (Optional) Name of the jurisdiction to filter for. None value returns data for all jurisdictions.
    '''
    
    if year_filter != None and date_field != None:
        df = df[df[date_field].dt.year == year_filter]

    if jurisdiction_filter != None and jurisdiction_field != None:
        df = df.query(jurisdiction_field + " = '" + jurisdiction_filter + "'")

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

        year-=1

    return years

if __name__ == "__main__":
    url = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2021-12/RIPA%20Stop%20Data%202020.csv"
    url = "https://www.projectcomport.org/department/4/complaints.csv"
    table1 = load_csv(url, limit=1026)
    table2 = load_csv(url)
    table3 = table2.head(1026)
    
