import pytest
import requests
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

import warnings
warnings.filterwarnings(action='ignore', module='arcgis')

def test_arcgis():
    lim = data_loaders.data_loader._default_limit
    data_loaders.data_loader._default_limit = 500
    data_loaders._verify_arcgis = True
    url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPDEmployeeDemographics/MapServer/0"
    gis = data_loaders.Arcgis(url)
    assert not gis.isfile()
    try:
        # Check if arcgis is installed
        from arcgis.features import FeatureLayerCollection
        # Verify that verify is True by getting active layer 
        _ = gis._Arcgis__active_layer
        # Load with verification
        gis.load()
        gis.get_count()
    except:
        pass

    data_loaders._verify_arcgis = False

    # Now load without verification as user would
    gis = data_loaders.Arcgis(url)
    # Confirm that verfication is not set
    with pytest.raises(AttributeError):
        gis._Arcgis__active_layer
    df = gis.load()
    count = gis.get_count()

    assert len(df)==count

    offset = 1
    nrows = count-offset
    df_offset = gis.load(nrows=nrows, offset=offset)
    assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

    df_offset = gis.load(offset=offset)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))
    
    data_loaders.data_loader._default_limit = lim

    try:
        from arcgis.features import FeatureLayerCollection
        last_slash = url.rindex("/")
        layer_num = url[last_slash+1:]
        base_url = url[:last_slash]
        layer_collection = FeatureLayerCollection(base_url)

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

        layer_query_result = active_layer.query(as_df=True)
    except:
        url += "/query"
        params = {}
        params["where"] = "1=1"
        params["outFields"] = "*"
        params["f"] = "json"

        r = requests.get(url, params=params)
        r.raise_for_status()

        features = r.json()["features"]
        params["resultOffset"] = len(features)
        r = requests.get(url, params=params)
        r.raise_for_status()

        features.extend(r.json()["features"])
        
        layer_query_result = pd.DataFrame.from_records([x["attributes"] for x in features])

    assert set(df.columns) == set(layer_query_result.columns)
    assert len(layer_query_result) == count
    df = df[layer_query_result.columns]

    assert layer_query_result.equals(df)

# Including all text date datasets for now. May want to include only unique date formats in the future
@pytest.mark.parametrize('url, year, date_field',[
    ('https://gis.ashevillenc.gov/server/rest/services/PublicSafety/APDCitations/MapServer/10', 2023, 'citation_date'), # YYYY-MM-DD
    ('https://xmaps.indy.gov/arcgis/rest/services/OpenData/OpenData_NonSpatial/MapServer/5', None, 'OCCURRED_DT'),# YYYY-MM-DD
    ('https://xmaps.indy.gov/arcgis/rest/services/OpenData/OpenData_NonSpatial/MapServer/6', None, 'OCCURRED_DT'),# YYYY-MM-DD
    ('https://xmaps.indy.gov/arcgis/rest/services/OpenData/OpenData_NonSpatial/MapServer/7', 2023, 'OCCURRED_DT'),# YYYY-MM-DD
    ('https://services.arcgis.com/aJ16ENn1AaqdFlqx/arcgis/rest/services/APDComplaints/FeatureServer/0', None, 'occurred_date'),  # M/D/YYYY
    ('https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/LMPD_STOPS_DATA_(2)/FeatureServer/0', 2020, 'ACTIVITY_DATE'),
    ('https://gis.ashevillenc.gov/server/rest/services/PublicSafety/APDIncidents/MapServer/3', 2023, 'date_occurred'),   # YYYYMMDD
    ('https://services.arcgis.com/aJ16ENn1AaqdFlqx/arcgis/rest/services/APD_ShowOfForce/FeatureServer/0', None, 'occ_date'),   # YYYYMMDD
    ("https://services.arcgis.com/aJ16ENn1AaqdFlqx/arcgis/rest/services/APDTrafficStops2020/FeatureServer/0", 2023, 'date_occurred'),    # YYYYMMDDHHMMSS
    ('https://services.arcgis.com/aJ16ENn1AaqdFlqx/arcgis/rest/services/APD_UseOfForce2021/FeatureServer/0', 2022, 'occurred_date'),  # YYYYMMDD.0
    ('https://services.arcgis.com/aJ16ENn1AaqdFlqx/arcgis/rest/services/APDUseOfForce/FeatureServer/0', None, 'date_occurred'), # MONTH, D, YYYY
    ('https://publicgis.tucsonaz.gov/open/rest/services/OpenData/OpenData_PublicSafety/MapServer/34/', None, 'INCI_DATE'),  # YYYY-MM-DDTHH:MM:SS.SSSZ
    ('https://gis.charlottenc.gov/arcgis/rest/services/ODP/CMPD_Calls_for_Service/MapServer/0', 2022, 'CALENDAR_YEAR'),  # YYYY
    ('https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPDOfficerInvolvedShootings_Incidents/MapServer/0', None, 'Year_Month'),      # YYYY-MM
    ("https://gis.charlottenc.gov/arcgis/rest/services/CMPD/Officer_Traffic_Stop/MapServer/0", 2020, 'Month_of_Stop'),
    ('https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPDOfficerInvolvedShootings_Officers/MapServer/0', None, 'YR'),   # Numeric year
])
def test_arcgis_text_date(url, year, date_field):
    loader = data_loaders.Arcgis(url, date_field=date_field)
    data = loader.load(year, pbar=False)
    
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message='Could not infer format.*')
        if isinstance(data[date_field].iloc[0],str) and data[date_field].iloc[0][-2:]=='.0': # Date is recorded as float
            dts = data[date_field].apply(lambda x: x[:-2] if isinstance(x,str) else None)
            dts = pd.to_datetime(dts, errors='coerce')
        else:
            dts = pd.to_datetime(data[date_field], errors='coerce', utc=True)
        if year:
            assert (dts.dt.year==year).all()

    un_months = dts.dt.month.unique()
    if len(un_months)==1 and un_months==[1] and (date_field.lower()=='yr' or 'year' in date_field.lower()):
        pass
    else:
        assert all([x in un_months for x in range(1,13)])

def test_arcgis_query():
    url = 'https://services1.arcgis.com/JLuzSHjNrLL4Okwb/arcgis/rest/services/Gilbert_Demographics/FeatureServer/0'

    loader = data_loaders.Arcgis(url)
    data = loader.load(pbar=False)
    data = data[data['Department']=='Police Department'.upper()].reset_index(drop=True)

    loader_query = data_loaders.Arcgis(url, query='{“Department”: “Police Department”}')
    data_query = loader_query.load(pbar=False)
    count = loader_query.get_count()

    loader_query = data_loaders.Arcgis(url, query='{“Department”: “Police Department”}')
    count2 = loader_query.get_count()

    assert len(data)==count
    assert count==count2
    pd.testing.assert_frame_equal(data, data_query)

def test_arcgis_geopandas():
    if _has_gpd:
        url = "https://services9.arcgis.com/kYvfX7YK8OobHItA/arcgis/rest/services/ARREST_CHARGES_2018_LAYER/FeatureServer/0"
        nrows = 1000
        df = data_loaders.Arcgis(url).load(nrows=nrows)

        assert type(df) == gpd.GeoDataFrame
    else:
        pass

def test_arcgis_pandas():
    data_loaders._use_gpd_force = False
    url = "https://services1.arcgis.com/wpJGOi6N4Rq5cqFv/arcgis/rest/services/Pursuits_2020_2021/FeatureServer/0/"
    date_field = "DATE"
    year_filter = 2020
    gis = data_loaders.Arcgis(url, date_field=date_field)
    df = gis.load(year=year_filter)
    count = gis.get_count(year=year_filter)
    # Reset
    data_loaders._use_gpd_force = None

    assert type(df) == pd.DataFrame
    assert len(df) == count

    count2 = gis.get_count(year=year_filter+1)

    # Ensure that count updates properly with different call (most recent count is cached)
    assert count!=count2
