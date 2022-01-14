import os
import geopandas as gpd
import pandas as pd
from sodapy import Socrata
import requests

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection
from arcgis.features.manage_data import extract_data

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: https://data.virginia.gov/profile/edit/developer_settings
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
defaultSodaPyKey = os.environ.get("SODAPY_API_KEY")

def loadGeoJSON(url, dateField=None, yearFilter=None, jurisdictionField=None, jurisdictionFilter=None):
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

    jsonData = response.json()

    df = gpd.GeoDataFrame.from_features(jsonData, crs=jsonData["crs"]["properties"]["name"], )

    if dateField != None:
        df = df.astype({dateField: 'datetime64[ns]'})

    df = filterDataFrame(df, dateField=dateField, yearFilter=yearFilter, 
        jurisdictionField=jurisdictionField, jurisdictionFilter=jurisdictionFilter)

    return df


def loadArcGIS(url, dateField=None, year=None):
    #TODO: Error checking for layer type
    layerCollection = FeatureLayerCollection(url)

    active_layer = layerCollection.layers[0]
    
    if dateField!=None and year!=None:
        if len(year)==1:
            startDate = str(year) + "-01-01"
            stopDate = str(year) + "-12-31"
        elif len(year)==2:
            if year[0] > year[1]:
                raise ValueError('year[0] needs to be smaller than or equal to year[1]')
            startDate = str(year[0]) + "-01-01"
            stopDate = str(year[1]) + "-12-31"
        else:
            raise ValueError('year needs to be a 1 or 2 argument value')
        
        where_query = f"{dateField} >= '{startDate}' AND  {dateField} < '{stopDate}'"
        print(f'where_query = {where_query}')
        layer_query_result = active_layer.query(where=where_query)
        print(f'len(layer_query_result) = {len(layer_query_result)}, layer_query_result.spatial_reference = {layer_query_result.spatial_reference}')
        if len(layer_query_result) > 0:
            layer_query_result.spatial_reference
            df = gpd.GeoDataFrame(layer_query_result.sdf,crs=layer_query_result.spatial_reference['wkid'])
            return df
        else:
            return None
    


def loadSocrataTable(url, data_set, dateField=None, year=None, optFilter=None, select=None, outputType=None, key=defaultSodaPyKey):
    # Load tables that use Socrata

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(url, key)

    limit = client.DEFAULT_LIMIT
    N = 1  # Initialize to value > 0 so while loop runs
    offset = 0

    where = ""
    if dateField!=None and year!=None:
        if not isinstance(year, list):
            year = [year, year]

        if len(year) > 2:
            raise ValueError("year should be a list of length 2: [startYear, stopYear]")

        startDate = str(year[0]) + "-01-01"
        stopDate  = str(year[1]) + "-12-31"
        where = dateField + " between '" + startDate + "' and '" + stopDate +"'"

    if optFilter is not None:
        if not isinstance(optFilter, list):
            optFilter = [optFilter]

        andStr = " AND "
        for filt in optFilter:
            where += andStr + filt

        if where[0:len(andStr)] == andStr:
            where = where[len(andStr):]

    while N > 0:
        results = client.get(data_set, where=where,
            limit=limit,offset=offset, select=select)

        if outputType == "set":
            if offset==0:
                df = set()

            if len(results)>0:
                filtKey = select.replace("DISTINCT ", "")
                results = [row[filtKey] for row in results if len(row)>0]
                results = set(results)
                df.update(results)

        elif outputType == "list":
            if offset==0:
                df = list()

            if len(results)>0:
                [df.append(row[select]) for row in results]

        elif outputType=="GeoDataFrame" or (outputType==None and len(results)>0 and "geolocation" in results[0]):
            outputType = "GeoDataFrame"
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
            outputType = "DataFrame"
            rows = pd.DataFrame.from_records(results)
            if offset==0:
                df = pd.DataFrame(rows)
            else:
                df = df.append(rows)

        N = len(results)
        offset += N

    return df


def filterDataFrame(df, dateField=None, yearFilter=None, jurisdictionField=None, jurisdictionFilter=None):
    if yearFilter != None and dateField != None:
        df = df[df[dateField].dt.year == yearFilter]

    if jurisdictionFilter != None and jurisdictionField != None:
        df = df.query(jurisdictionField + " = '" + jurisdictionFilter + "'")

    return df