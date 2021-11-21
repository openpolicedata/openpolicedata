import os
import geopandas as gpd
import pandas as pd
from sodapy import Socrata

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: https://data.virginia.gov/profile/edit/developer_settings
# Copy the App Token
# Create a file with name below in the folder where this file is located and copy
# the app token into it
_sodapyKeyFilename = "sodapy_api.key"

def _getDefaultSodaPyKey():
    key = None
    if os.path.isfile(_sodapyKeyFilename):
        with open(_sodapyKeyFilename, "rt") as f:
            key = f.readline().strip()

    return key

defaultSodaPyKey = _getDefaultSodaPyKey()

def loadSocrataTable(url, data_set, dateField=None, year=None, optFilter=None, key=defaultSodaPyKey):
    # Load tables that use Socrata

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(url, key)

    limit = 10000
    N = limit
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

    hasGeom = False
    while N > 0:
        results = client.get(data_set, where=where,
            limit=limit,offset=offset)

        if hasGeom or (len(results)>0 and "geolocation" in results[0]):
            hasGeom = True
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
            rows = pd.DataFrame.from_records(results)
            if offset==0:
                df = pd.DataFrame(rows)
            else:
                df.append(rows)

        N = len(results)
        offset += limit

    return df

