from io import BytesIO
import pytest
import re
import requests
if __name__ == "__main__":
	import sys
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

def test_process_date_input_empty():
    with pytest.raises(ValueError):
        data_loaders._process_date([])
    
def test_process_date_too_many():
    year = [2021, 2022, 2023]
    with pytest.raises(ValueError):
        data_loaders._process_date(year)

def test_process_dates_year_input_wrong_order():
    year = [2023, 2021]
    with pytest.raises(ValueError):
        data_loaders._process_date(year)

@pytest.mark.parametrize("url, dataset", [
    ("https://wallkillpd.org/document-center/data/vehicle-a-pedestrian-stops/2016-vehicle-a-pedestrian-stops", 
        "147-2016-2nd-quarter-vehicle-a-pedestrian-stops/file.html; 148-2016-3rd-quarter-vehicle-a-pedestrian-stops/file.html; 167-2016-4th-quarter-vehicle-pedestrian-stops/file.html"), 
    ("http://www.bremertonwa.gov/DocumentCenter/View", 
        "*arrest*| 4713/January-2017-XLSX; 4873/February-2017-XLSX; 4872/March-2017-XLSX; https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Washington_Bremerton_ARRESTS_April_2017.csv; 5026/May-2017-XLSX; 5153/June-2017-XLSX; 5440/July-2017-XLSX; 5441/August-2017-XLSX; 5477/September-2017-XLSX; 5548/October-2017-XLSX; 5608/November-2017-XLSX; 5607/December-2017-XLSX"),
    ("https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments", 
        'Open YTD & Closed YTD; New Lawsuits & Closed Lawsuits & New Claims & Closed Claims| New%20and%20Closed%20Lawsuits%20CY%202023%20as%20of%207.20.2023.xlsx; New%20and%20Closed%20Lawsuits%20and%20Claims%202023%20July-December%20External.xlsx')
    ]
    )
def test_combined(url, dataset):
    loader = data_loaders.CombinedDataset(data_loaders.Excel, url, dataset)

    assert loader.isfile()

    sheets = None
    if '|' in dataset:
        dataset = dataset.split('|')
        assert len(dataset)==2
        sheets = dataset[0].split(';')
        dataset = dataset[1]

    dfs = []
    for k,ds in enumerate(dataset.split(';')):
        ds = ds.strip()
        headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}
        for s in sheets[min(k,len(sheets)-1)].split('&') if sheets else [0]:
            if ds.endswith('.csv'):
                df = pd.read_csv(ds)
            else:
                r = requests.get(url + '/' + ds, stream=True, headers=headers)
                r.raise_for_status()
                file_like = BytesIO(r.content)
                if isinstance(s,str):
                    s = s.strip()
                    if '*' in s:
                        all_sheets = pd.ExcelFile(file_like).sheet_names
                        p = s.replace('*','.*')
                        s = [x for x in all_sheets if re.search(p,x)]
                        assert len(s)==1
                        s = s[0]
                df = pd.read_excel(file_like, sheet_name=s)
            cols = [x for x in df.columns if not x.startswith('Unnamed')]
            df = df[cols]
            dfs.append(df)
    df_true = pd.concat(dfs, ignore_index=True).convert_dtypes()

    count = loader.get_count(force=True)

    assert len(df_true) == count

    df = loader.load().convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true)

    offset = 3000
    nrows = 20
    df = loader.load(offset=offset).convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true.iloc[offset:].convert_dtypes())

    df = loader.load(nrows=nrows).convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true.head(nrows).convert_dtypes())

    df = loader.load(offset=offset, nrows=nrows).convert_dtypes()
    pd.testing.assert_frame_equal(df, df_true.iloc[offset:].head(nrows).convert_dtypes())
    

def test_ckan():
    lim = data_loaders._default_limit
    data_loaders._default_limit = 500
    url = "https://data.virginia.gov"
    dataset = "60506bbb-685f-4360-8a8c-30e137ce3615"
    date_field = "STOP_DATE"
    agency_field = 'AGENCY NAME'
    loader = data_loaders.Ckan(url, dataset, date_field)

    assert not loader.isfile()

    count = loader.get_count()

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"')
    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    year = 2022
    count = loader.get_count(year=year)

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"' + 
                        f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01'""")
    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    agency='William and Mary Police Department'
    opt_filter = {'=':{agency_field:agency}}
    opt_filter = 'LOWER("' + agency_field + '")' + " = '" + agency.lower() + "'"
    count = loader.get_count(year=year, opt_filter=opt_filter)

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"' + 
                        f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01' AND """+
                        opt_filter)
    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    loader._last_count = None
    df = loader.load(year=year, pbar=False, opt_filter=opt_filter)

    assert len(df)==count

    offset = 1
    nrows = count - 2
    df_offset = loader.load(year=year, nrows=nrows, offset=1, pbar=False, opt_filter=opt_filter)

    assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

    df_offset = loader.load(year=year, offset=1, pbar=False, opt_filter=opt_filter)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}"' + 
                        f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01' AND """+
                        opt_filter + " ORDER BY _id")
    df_comp= pd.DataFrame(r.json()['result']['records'])
    df_comp[date_field] = pd.to_datetime(df_comp[date_field])
    df_comp = df_comp.drop(columns=['_id','_full_text'])
    
    assert df.equals(df_comp)

    data_loaders._default_limit = lim


def test_carto():
    lim = data_loaders._default_limit
    data_loaders._default_limit = 500
    url = "phl"
    dataset = "shootings"
    date_field = "date_"
    loader = data_loaders.Carto(url, dataset, date_field)

    assert not loader.isfile()

    count = loader.get_count()

    r = requests.get(f"https://phl.carto.com/api/v2/sql?q=SELECT count(*) FROM {dataset}")
    r.raise_for_status()
    assert count==r.json()["rows"][0]["count"]>0

    year = 2019
    count = loader.get_count(year=year)

    r = requests.get(f"https://phl.carto.com/api/v2/sql?q=SELECT count(*) FROM {dataset} WHERE {date_field} >= '{year}-01-01' AND {date_field} < '{year+1}-01-01'")
    r.raise_for_status()
    assert count==r.json()["rows"][0]["count"]>0

    df = loader.load(year=year, pbar=False)

    assert len(df)==count

    offset = 1
    nrows = count - 2
    df_offset = loader.load(year=year, nrows=nrows, offset=1, pbar=False)

    assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

    df_offset = loader.load(year=year, offset=1, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    r = requests.get(f"https://phl.carto.com/api/v2/sql?format=GeoJSON&q=SELECT * FROM {dataset} WHERE {date_field} >= '{year}-01-01' AND {date_field} < '{year+1}-01-01'")
    features = r.json()["features"]
    df_comp= pd.DataFrame.from_records([x["properties"] for x in features])
    df_comp[date_field] = pd.to_datetime(df_comp[date_field])
    
    try:
        import geopandas as gpd
        from shapely.geometry import Point
        geometry = []
        for feat in features:
            if "geometry" not in feat or feat["geometry"]==None or len(feat["geometry"]["coordinates"])<2:
                geometry.append(None)
            else:
                geometry.append(Point(feat["geometry"]["coordinates"][0], feat["geometry"]["coordinates"][1]))

        df_comp = gpd.GeoDataFrame(df_comp, crs=4326, geometry=geometry)
    except:
        geometry = [feat["geometry"] if "geometry" in feat else None for feat in features]
        df_comp["geolocation"] = geometry

    assert df.equals(df_comp)

    data_loaders._default_limit = lim

    if data_loaders._has_gpd:
        assert type(df) == gpd.GeoDataFrame
        data_loaders._has_gpd = False
        df = loader.load(year=year, nrows=nrows, pbar=False)
        data_loaders._has_gpd = True
        assert isinstance(df, pd.DataFrame)

    url2 = "https://phl.carto.com/api/v2/sql?"
    loader2 = data_loaders.Carto(url2, dataset, date_field)
    assert loader.url==loader2.url

def test_arcgis():
    lim = data_loaders._default_limit
    data_loaders._default_limit = 500
    data_loaders._verify_arcgis = True
    url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16"
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
    
    data_loaders._default_limit = lim

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
    ('https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/11', None, 'YEAR_MONTH'),      # YYYY-MM
    ("https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/14", 2020, 'Month_of_Stop'),
    ('https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/13', None, 'YR'),   # Numeric year
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

# No datasets currently trigger usage of the legacy server code
# def test_arcgis_legacy_server():
#     url = "https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/Arrest/FeatureServer/0/" 

#     gis = data_loaders.Arcgis(url)
#     gis.load(nrows=1)

#     url = "https://xmaps.indy.gov/arcgis/rest/services/OpenData/OpenData_NonSpatial/MapServer/5/" 
#     gis = data_loaders.Arcgis(url)
#     gis.load(nrows=1)


def test_arcgis_geopandas():
    if _has_gpd:
        url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
        date_field = "TIME_PHONEPICKUP"
        year_filter = 2020
        nrows = 1000
        df = data_loaders.Arcgis(url, date_field=date_field).load(year=year_filter, nrows=nrows)

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

def test_socrata_geopandas():
    if _has_gpd:
        url = "data.montgomerycountymd.gov"
        data_set = "4mse-ku6q"
        date_field = "date_of_stop"
        year = 2020
        nrows = 1000
        df = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field).load(year=year, nrows=nrows)

        assert type(df) == gpd.GeoDataFrame
    else:
        pass

def test_socrata_pandas():
    data_loaders._use_gpd_force = False
    url = "data.montgomerycountymd.gov"
    data_set = "usip-62e2"
    date_field = "created_dt"
    year = 2020
    loader = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field)
    df = loader.load(year=year, pbar=False)
    count = loader.get_count(year=year)

    # Reset
    data_loaders._use_gpd_force = None

    assert type(df) == pd.DataFrame
    assert len(df) == count

    count2 = loader.get_count(year=year+1)

    # Ensure that count updates properly with different call (most recent count is cached)
    assert count!=count2

def test_socrata():
    lim = data_loaders._default_limit
    data_loaders._default_limit = 500
    url = "data.austintexas.gov"
    data_set = "sc8s-w4ka"
    loader = data_loaders.Socrata(url, data_set)
    df =loader.load(pbar=False)
    assert not loader.isfile()
    count = loader.get_count()

    assert len(df)==count

    offset = 1
    nrows = len(df)-offset-1
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert set(df.columns)==set(df_offset.columns)
    df_offset = df_offset[df.columns]
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))

    df_offset = loader.load(offset=offset, pbar=False)
    assert set(df.columns)==set(df_offset.columns)
    df_offset = df_offset[df.columns]
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))
    
    data_loaders._default_limit = lim

    client = data_loaders.SocrataClient(url, data_loaders.default_sodapy_key, timeout=60)
    results = client.get(data_set, order=":id", limit=100000)
    rows = pd.DataFrame.from_records(results)

    assert len(df) == count
    assert rows.equals(df)

def test_csv():
    url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
    date_field = "INCIDENT_DATE"
    loader = data_loaders.Csv(url, date_field=date_field)
    assert loader.isfile()
    df = loader.load(pbar=False)

    offset = 1
    nrows = len(df)-offset-1
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
    
    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    df_comp = pd.read_csv(url)
    df_comp = df_comp.astype({date_field: 'datetime64[ns]'})
    df = df.astype({date_field: 'datetime64[ns]'})

    count = loader.get_count()
    assert len(df_comp) == count
    # Test using cached value
    assert count == loader.get_count()

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = df.astype({date_field: 'datetime64[ns]'})
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

    nrows = 7
    df = data_loaders.Csv(url).load(nrows=nrows)
    df_comp = pd.read_csv(url, nrows=nrows)

    assert df_comp.equals(df)


def test_csv_year_filter():
    url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
    loader = data_loaders.Csv(url, date_field="INCIDENT_DATE")
    year = 2020
    df = loader.load(year=year, pbar=False)
    with pytest.raises(ValueError):
        count = loader.get_count(year=year)

    count = loader.get_count(year=year, force=True)
    assert len(df) == count

    count2 = loader.get_count(year=year+1, force=True)

    # Ensure that count updates properly with different call (most recent count is cached)
    assert count!=count2


def test_excel():
    url = "https://www.norristown.org/DocumentCenter/View/1789/2017-2018-Use-of-Force"
    date_field = "Date"
    loader = data_loaders.Excel(url, date_field=date_field, data_set='2017-2018')
    assert loader.isfile()
    df = loader.load(pbar=False)

    offset = 1
    nrows = len(df)-offset-1
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))

    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    df_comp = pd.read_excel(url)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]

    with pytest.raises(ValueError):
        count = loader.get_count()
    count = loader.get_count(force=True)
    assert len(df_comp) == count

    # Testing 2nd call which should used cached value
    assert count == loader.get_count(force=True)

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = df.astype({date_field: 'datetime64[ns]'})
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

    nrows = 7
    df = loader.load(nrows=nrows, pbar=False)        
    df_comp = pd.read_excel(url, nrows=nrows)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    assert df_comp.equals(df)


def test_excel_year_sheets(skip):
    if "Northampton" in skip:
        return

    url = "https://northamptonpd.com/images/ODP%20Spreadsheets/2014-2020_MV_Pursuits_incident_level_data.xlsx"
    loader = data_loaders.Excel(url, date_field="Date")

    years = loader.get_years()
    assert years == [x for x in range(2014,2021)]

    df_comp = pd.read_excel(url, sheet_name="2014")
    df_comp.columns= [x for x in df_comp.iloc[0]]
    df_comp.drop(index=df_comp.index[0], inplace=True)
    df_comp.reset_index(drop=True, inplace=True)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df_comp = df_comp.iloc[:, 1:]

    # Load all years
    df_2014 = loader.load(year=2014, pbar=False)

    assert df_comp.equals(df_2014)

    df_comp = pd.read_excel(url, sheet_name="2015")
    df_comp.columns= [x for x in df_comp.iloc[0]]
    df_comp.drop(index=df_comp.index[0], inplace=True)
    df_comp.reset_index(drop=True, inplace=True)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df_comp = df_comp.iloc[:, 1:]

    # Load all years
    df_2015 = loader.load(year=2015, pbar=False)

    assert df_comp.equals(df_2015)

    # Note: There is no 2013 data
    df_multi = loader.load(year=[2013,2015], pbar=False)

    assert df_multi.equals(pd.concat([df_2014, df_2015], ignore_index=True))

    df = loader.load(pbar=False)
    df_last = loader.load(year=years[-1], pbar=False)

    assert df.head(len(df_multi)).equals(df_multi)
    assert df.tail(len(df_last)).reset_index(drop=True).equals(df_last.reset_index(drop=True))

    # Test loading to ensure that channel name changes are handled
    data_loaders.Excel("https://northamptonpd.com/images/ODP%20Spreadsheets/NPD_Use_of_Force_2014-2020_incident_level_data.xlsx").load()

def test_excel_header():
    url = "https://cms7files1.revize.com/sparksnv/Document_Center/Sparks%20Police/IA%20Data/2000-2022-SPD-OIS-Incidents%20(3).xlsx"

    loader = data_loaders.Excel(url)
    df = loader.load(pbar=False)

    df_comp = pd.read_excel(url)
    df_comp.columns= [x for x in df_comp.iloc[3]]
    df_comp.drop(index=df_comp.index[0:4], inplace=True)
    df_comp.reset_index(drop=True, inplace=True)
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df_comp = df_comp.dropna(thresh=10)

    assert(df_comp.equals(df))


def test_excel_xls():
    url = r"http://gouda.beloitwi.gov/WebLink/0/edoc/66423/3Use%20of%20Force%202017%20-%20last%20updated%201-12-18.xls"

    try:
        df_comp = pd.read_excel(url)
    except Exception as e:
        if len(e.args) and e.args[0]=='Excel file format cannot be determined, you must specify an engine manually.':
            r = requests.get(url)
            r.raise_for_status()
            text = r.content
            file_like = BytesIO(text)
            df_comp = pd.read_excel(file_like)
        else:
            raise e
    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    df = data_loaders.Excel(url).load()

    assert df_comp.equals(df)


def test_excel_xls_protected():
    url = "http://www.rutlandcitypolice.com/app/download/5136813/ResponseToResistance+2015-2017.xls"

    r = requests.get(url)
    r.raise_for_status()

    import os
    import msoffcrypto
    import tempfile
    # Create a file path by joining the directory name with the desired file name
    output_directory = tempfile.gettempdir()
    file_path = os.path.join(output_directory, 'temp1.xls')

    # Write the file
    with open(file_path, 'wb') as output:
        output.write(r.content)

    file_path_decrypted = os.path.join(output_directory, 'temp2.xls')
    # Try and unencrypt workbook with magic password
    fp = open(file_path, 'rb')
    wb_msoffcrypto_file = msoffcrypto.OfficeFile(fp)

    # https://stackoverflow.com/questions/22789951/xlrd-error-workbook-is-encrypted-python-3-2-3
    # https://nakedsecurity.sophos.com/2013/04/11/password-excel-velvet-sweatshop/
    wb_msoffcrypto_file.load_key(password='VelvetSweatshop')
    with open(file_path_decrypted, 'wb') as output:
        wb_msoffcrypto_file.decrypt(output)

    fp.close()

    df_comp = pd.read_excel(open(file_path_decrypted, 'rb'))

    os.remove(file_path)
    os.remove(file_path_decrypted)

    loader = data_loaders.Excel(url)
    df = loader.load(pbar=False)

    df_comp = df_comp.convert_dtypes()
    df_comp.columns = [x.strip() if isinstance(x, str) else x for x in df_comp.columns]
    assert df_comp.equals(df)

if __name__ == "__main__":

    test_combined(None, None, None, None, None)
    # test_ckan(None,None,None,None,None)
    # test_carto(None,None,None,None,None)
    # test_arcgis(None,None,None,None,None)
    # test_arcgis_geopandas(None,None,None,None,None)
    # test_arcgis_pandas(None,None,None,None,None)
    # test_csv(None,None,None,None,None)
    # test_csv_year_filter(None,None,None,None,None)
    # test_process_date_input_empty(None,None,None,None,None)
    # test_process_date_too_many(None,None,None,None,None)
    # test_process_dates_year_input_wrong_order(None,None,None,None,None)
    # test_socrata(None,None,None,None,None)
    # test_socrata_geopandas(None,None,None,None,None)
    # test_socrata_pandas(None,None,None,None,None)
    # test_excel(None,None,None,None,None)
    # test_excel_year_sheets(None,None,None,None,None)
    # test_excel_header(None,None,None,None,None)
    # test_excel_xls(None,None,None,None,None)
    # test_excel_xls_protected(None,None,None,None,None)