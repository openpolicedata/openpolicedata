import os
import geopandas as gpd
import pandas as pd
from sodapy import Socrata

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: https://data.virginia.gov/profile/edit/developer_settings
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
defaultSodaPyKey = os.environ.get("SODAPY_API_KEY")

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
        startDate = str(year) + "-01-01"
        stopDate = str(year) + "-12-31"
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
                results = [row[select.replace("DISTINCT ", "")] for row in results]
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

