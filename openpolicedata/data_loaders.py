import os
from datetime import date
import geopandas as gpd
import pandas as pd
from sodapy import Socrata
import requests

from arcgis.features import FeatureLayerCollection

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: https://data.virginia.gov/profile/edit/developer_settings
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
default_sodapy_key = os.environ.get("SODAPY_API_KEY")

def load_csv(url, date_field=None, year_filter=None, jurisdiction_field=None, jurisdiction_filter=None):
    table = pd.read_csv(url, parse_dates=True)
    table = filter_dataframe(table, date_field=date_field, year_filter=year_filter, 
        jurisdiction_field=jurisdiction_field, jurisdiction_filter=jurisdiction_filter)

    return table


def load_geojson(url, date_field=None, year_filter=None, jurisdiction_field=None, jurisdiction_filter=None):
    try:
        response = requests.get(url)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    # pylint: disable=undefined-variable. 
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.6
        Exception()
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6
        Exception()

    json_data = response.json()

    df = gpd.GeoDataFrame.from_features(json_data, crs=json_data["crs"]["properties"]["name"], )

    if date_field != None:
        df = df.astype({date_field: 'datetime64[ns]'})

    df = filter_dataframe(df, date_field=date_field, year_filter=year_filter, 
        jurisdiction_field=jurisdiction_field, jurisdiction_filter=jurisdiction_filter)

    return df


def load_arcgis(url, date_field=None, year=None, limit=None):
    #TODO: Error checking for layer type
    layer_collection = FeatureLayerCollection(url)

    active_layer = layer_collection.layers[0]
    
    where_query = ""
    if date_field!=None and year!=None:
        if not isinstance(year, list):
            year = [year, year]

        if len(year)==2:
            if year[0] > year[1]:
                raise ValueError('year[0] needs to be smaller than or equal to year[1]')
            start_date = str(year[0]) + "-01-01"
            stop_date = str(year[1]+1) + "-01-01"
        else:
            raise ValueError('year needs to be a 1 or 2 argument value')
        
        where_query = f"{date_field} >= '{start_date}' AND  {date_field} < '{stop_date}'"
        layer_query_result = active_layer.query(where=where_query, return_all_records=(limit == None), result_record_count=limit)
    else:
        layer_query_result = active_layer.query(return_all_records=(limit == None), result_record_count=limit)

    if len(layer_query_result) > 0:
        return gpd.GeoDataFrame(layer_query_result.sdf,crs=layer_query_result.spatial_reference['wkid'])
    else:
        return []


def load_socrata(url, data_set, date_field=None, year=None, opt_filter=None, select=None, output_type=None, 
                 limit=None, key=default_sodapy_key):
    # Load tables that use Socrata

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
        if not isinstance(year, list):
            year = [year, year]

        if len(year) > 2:
            raise ValueError("year should be a list of length 2: [startYear, stopYear]")

        start_date = str(year[0]) + "-01-01"
        stop_date  = str(year[1]) + "-12-31"
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


def filter_dataframe(df, date_field=None, year_filter=None, jurisdiction_field=None, jurisdiction_filter=None):
    if year_filter != None and date_field != None:
        df = df[df[date_field].dt.year == year_filter]

    if jurisdiction_filter != None and jurisdiction_field != None:
        df = df.query(jurisdiction_field + " = '" + jurisdiction_filter + "'")

    return df


def get_years_argis(url, date_field):
    return _get_years("arcgis", url, date_field=date_field)


def get_years_socrata(url, data_set, date_field):
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

        if len(df)==0:
            misses+=1
        else:
            misses = 0
            max_misses = max_misses_gap
            years.append(year)

        year-=1

    return years
