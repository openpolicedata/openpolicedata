from datetime import datetime
from io import BytesIO
import json
import pytest
import re
import requests
import sys
from zipfile import ZipFile

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, dataset_id
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

import warnings
warnings.filterwarnings(action='ignore', module='arcgis')

@pytest.mark.parametrize('url',['https://stacks.stanford.edu/file/druid:yg821jf8611/yg821jf8611_ar_little_rock_2020_04_01.csv.zip',
                                'https://www.chicagopolice.org/wp-content/uploads/legacy/2016-ISR.zip'])
def test_load_single_file_csv_zip(all_datasets, url):
    ds = all_datasets[all_datasets['URL']==url]
    assert len(ds)==1
    if pd.notnull(ds['py_min_version'].iloc[0]):
        if sys.version_info<=tuple(int(x) for x in ds['py_min_version'].iloc[0].split('.')):
            # Dataset does not work in this Python version
            return

    loader = data_loaders.Csv(url)
    df = loader.load(pbar=False)

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
        df_true = pd.read_csv(url)
    except:
        df_true = pd.read_csv(url,encoding_errors='surrogateescape', storage_options=headers)
    assert df.equals(df_true)

def test_load_multi_excel_file_zip():
    url = 'https://cdn.muckrock.com/foia_files/2024/05/29/evidence.com_case_P008746_package_1_of_1_created_2024-05-28T12_58_42Z.zip'
    file = 'data1.xlsx'
    loader = data_loaders.Excel(url, data_set=file)
    df = loader.load(pbar=False)

    r = requests.get(url, stream=True)
    r.raise_for_status()
    b = BytesIO()
    for data in r.iter_content(100000):
        b.write(data)

    b.seek(0)
    z = ZipFile(b, 'r')
    df_true = pd.read_excel(BytesIO(z.read(file)))
    df_true = df_true.convert_dtypes()

    assert df.equals(df_true)

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
    '{"urls": ["147-2016-2nd-quarter-vehicle-a-pedestrian-stops/file.html", "148-2016-3rd-quarter-vehicle-a-pedestrian-stops/file.html", "167-2016-4th-quarter-vehicle-pedestrian-stops/file.html"]}'), 
    ("http://www.bremertonwa.gov/DocumentCenter/View", 
    '{"sheets": "*arrest*", "urls": ["4713/January-2017-XLSX", "4873/February-2017-XLSX", "4872/March-2017-XLSX", "https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Washington_Bremerton_ARRESTS_April_2017.csv", "5026/May-2017-XLSX", "5153/June-2017-XLSX", "5440/July-2017-XLSX", "5441/August-2017-XLSX", "5477/September-2017-XLSX", "5548/October-2017-XLSX", "5608/November-2017-XLSX", "5607/December-2017-XLSX"]}'),
    ("https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments", 
    '[{"urls": ["New and Closed Lawsuits CY 2023 as of 7.20.2023.xlsx"], "sheets": ["Open YTD", "Closed YTD"]}, {"urls": ["New and Closed Lawsuits and Claims 2023 July-December External.xlsx"], "sheets": ["New Lawsuits", "Closed Lawsuits", "New Claims", "Closed Claims"]}]')
    ]
    )
def test_combined(url, dataset):
    dataset_dict = dataset_id.parse_id(dataset)
    exp_datasets = dataset_id.expand(dataset_dict)
    assert dataset_id.is_combined_dataset(exp_datasets)
    loader = data_loaders.CombinedDataset(data_loaders.Excel, url, exp_datasets, pbar=False)

    assert loader.isfile()

    dataset_dict_gt = json.loads(dataset)
    if isinstance(dataset_dict_gt, dict) and set(dataset_dict_gt.keys())=={'urls'}:
        exp_datasets_gt = [{'url':x} for x in dataset_dict_gt['urls']]
    elif isinstance(dataset_dict_gt, dict) and set(dataset_dict_gt.keys())=={'urls','sheets'}:
        exp_datasets_gt = [{'url':x, 'sheets':[dataset_dict_gt['sheets']]} for x in dataset_dict_gt['urls']]
    elif isinstance(dataset_dict_gt, list) and set(dataset_dict_gt[0].keys())=={'urls','sheets'} and len(dataset_dict_gt[0]['urls'])==1:
        exp_datasets_gt = [{'url':x['urls'][0], 'sheets':x['sheets']} for x in dataset_dict_gt]
    else:
        raise NotImplementedError()

    dfs = []
    for ds in exp_datasets_gt:
        headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}
        if 'url' in ds and ds['url'].endswith('.csv'):
            dfcur = pd.read_csv(ds['url'])
            cols = [x for x in dfcur.columns if not x.startswith('Unnamed')]
            dfcur = dfcur[cols]
            dfs.append(dfcur)
        else:
            url_cur = url + '/' + ds['url'] if 'url' in ds else url
            r = requests.get(url_cur, stream=True, headers=headers)
            r.raise_for_status()
            file_like = BytesIO(r.content)
            sheets = ds['sheets'] if 'sheets' in ds else [0]
            for s in sheets:
                if isinstance(s, str):
                    s = s.strip()
                    if '*' in s:
                        all_sheets = pd.ExcelFile(file_like).sheet_names
                        p = s.replace('*','.*')
                        s = [x for x in all_sheets if re.search(p,x)]
                        assert len(s)==1
                        s = s[0]

                dfcur = pd.read_excel(file_like, sheet_name=s)

                cols = [x for x in dfcur.columns if not x.startswith('Unnamed')]
                dfcur = dfcur[cols]
                dfs.append(dfcur)
        
    df_true = pd.concat(dfs, ignore_index=True).convert_dtypes()

    count = loader.get_count(force=True)
    df = loader.load().convert_dtypes()

    assert len(df_true) == count
    
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

    r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}" '+
                     'ORDER BY "_id" OFFSET 0 LIMIT 100')
    date_col_info = [x for x in r.json()['result']["fields"] if x["id"]==date_field]
    if len(date_col_info)==0:
        raise ValueError(f"Date column {date_field} not found")
    filter_year = date_col_info[0]["type"] not in ['timestamp','date']

    year = 2022
    count = loader.get_count(year=year)

    if filter_year:
        def gen_where(year):
            return '"' + date_field + '"' + rf" LIKE '%{year}%'"

        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}" WHERE ' + 
                            gen_where(year))
    else:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"' + 
            f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01'""")

    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    agency='William and Mary Police Department'
    opt_filter = {'=':{agency_field:agency}}
    opt_filter = 'LOWER("' + agency_field + '")' + " = '" + agency.lower() + "'"
    count = loader.get_count(year=year, opt_filter=opt_filter)

    if filter_year:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}" WHERE ' + 
                        gen_where(year) + " AND "+ opt_filter)
    else:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT COUNT(*) FROM "{dataset}"' + 
                    f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01' AND """+
                    opt_filter)
    r.raise_for_status()
    assert count==r.json()['result']['records'][0]['count']>0

    loader._last_count = None
    df = loader.load(year=year, pbar=False, opt_filter=opt_filter)

    assert len(df)==count
    assert (df[agency_field]==agency).all()

    offset = 1
    nrows = count - 2
    df_offset = loader.load(year=year, nrows=nrows, offset=1, pbar=False, opt_filter=opt_filter)

    assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

    df_offset = loader.load(year=year, offset=1, pbar=False, opt_filter=opt_filter)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    if filter_year:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}" WHERE ' + 
                        gen_where(year) + " AND "+ opt_filter + " ORDER BY _id")
    else:
        r = requests.get(f'https://data.virginia.gov/api/3/action/datastore_search_sql?sql=SELECT * FROM "{dataset}"' + 
                            f""" WHERE "{date_field}" >= '{year}-01-01' AND "{date_field}" < '{year+1}-01-01' AND """+
                            opt_filter + " ORDER BY _id")
    df_comp= pd.DataFrame(r.json()['result']['records'])
    if not filter_year:
        df_comp[date_field] = pd.to_datetime(df_comp[date_field])
    df_comp = df_comp.drop(columns=['_id','_full_text'])
    
    assert df.equals(df_comp)

    cur_year = datetime.now().year
    year_range = [cur_year-1, cur_year]
    df = loader.load(year=year_range, pbar=False, opt_filter=opt_filter)
    assert (df[agency_field]==agency).all()

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

def test_socrata_agency_filter():
    url = "data.ct.gov/"
    dataset = "nahi-zqrt"
    date_field = "interventiondatetime"
    agency_field = 'department_name'
    loader = data_loaders.Socrata(url, dataset, date_field)

    agency='Winsted'
    opt_filter = 'LOWER(' + agency_field + ") = '" + agency.lower() + "'"
    df = loader.load(year=2018, opt_filter=opt_filter)

    assert (df[agency_field]==agency).all()

    df = loader.load(year=[2018,2019], opt_filter=opt_filter)

    assert (df[agency_field]==agency).all()

def test_socrata():
    lim = data_loaders._default_limit
    data_loaders._default_limit = 500
    url = "www.transparentrichmond.org"
    data_set = "asfd-zcvn"
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

# Another CSV with newline characters: https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Texas_Austin_OFFICER-INVOLVED_SHOOTINGS-INCIDENTS.csv
@pytest.mark.parametrize('url, date_field, query',[
    ('https://public.tableau.com/views/PPBOpenDataDownloads/OIS-All.csv?:showVizHome=no', 'Day of Date Time', None),
    ("https://opendata.jaxsheriff.org/OIS/Export", "IncidentDate", None),
    ('https://raw.githubusercontent.com/openpolicedata/opd-datasets/refs/heads/main/data/Wisconsin_Milwaukee_COMPLAINTS.csv', 'DateReported', '{“Department”:”Milwaukee Police Department”}'),
    ('https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Texas_Austin_OFFICER-INVOLVED_SHOOTINGS-INCIDENTS.csv', 'date', None),
])
def test_csv(url, date_field, query):
    if pd.notnull(query):
        # Remove any curly quotes
        query = query.replace('“','"').replace('”','"')
        query = json.loads(query)

    loader = data_loaders.Csv(url, date_field=date_field, query=query)
    assert loader.isfile()
    df = loader.load(pbar=False)

    offset = 1
    nrows = len(df)-offset
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
    
    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    df_comp = pd.read_csv(url)
    if bool(query):
        for k,v in query.items():
            df_comp = df_comp[df_comp[k]==v].reset_index(drop=True)

    def convert_date_field(df):
        df[date_field] = df[date_field].apply(lambda x: re.sub(r'(\d\d)20(\d\d)', r'\1/20\2', x) if isinstance(x,str) else x)
        return df.astype({date_field: 'datetime64[ns]'})
    
    df_comp = convert_date_field(df_comp)
    df = convert_date_field(df)

    count = loader.get_count(force=True)
    assert len(df_comp) == count
    # Test using cached value
    assert count == loader.get_count()

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = convert_date_field(df)
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

    if not query:
        nrows = 7
        df = data_loaders.Csv(url).load(nrows=nrows)
        df_comp = pd.read_csv(url, nrows=nrows)

        assert df_comp.equals(df)

def test_html():
    url = "https://www.openpolicedata.com/StJohnIN/Citations/2023Citations.php"
    date_field = "Date"
    loader = data_loaders.Html(url, date_field=date_field)
    assert loader.isfile()
    df = loader.load(pbar=False)

    # Ensure entire datasets is read in
    assert df[date_field].min() == pd.Timestamp(f"{df[date_field].dt.year.mode().iloc[0]}-01-01")
    assert df[date_field].max() == pd.Timestamp(f"{df[date_field].dt.year.mode().iloc[0]}-12-30")
    assert len(df)==2510

    offset = 1
    nrows = len(df)-offset-1
    df_offset = loader.load(offset=offset,nrows=nrows, pbar=False)
    assert df_offset.equals(df.iloc[offset:nrows+offset].reset_index(drop=True))
    
    df_offset = loader.load(offset=offset, pbar=False)
    assert df_offset.equals(df.iloc[offset:].reset_index(drop=True))

    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'}
    df_comp = pd.read_html(url, storage_options=header)[0]
    df_comp = df_comp.astype({date_field: 'datetime64[ns]'})
    df = df.astype({date_field: 'datetime64[ns]'})

    with pytest.raises(ValueError):
        count = loader.get_count()

    count = loader.get_count(force=True)
    assert len(df_comp) == count
    # Test using cached value
    assert count == loader.get_count()

    assert df_comp.equals(df)

    with pytest.raises(ValueError):
        loader.get_years()

    years = loader.get_years(force=True)

    df = df.astype({date_field: 'datetime64[ns]'})
    assert list(df[date_field].dt.year.sort_values(ascending=True).dropna().unique()) == years

@pytest.mark.parametrize('url, date_field',[
    ('https://public.tableau.com/views/PPBOpenDataDownloads/OIS-All.csv?:showVizHome=no', 'Day of Date Time'),
    ("https://opendata.jaxsheriff.org/OIS/Export", "IncidentDate")
])
def test_csv_year_filter(url, date_field):
    loader = data_loaders.Csv(url, date_field=date_field)
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

# Add Norwich OIS and UoF to Unnamed column test
# Unnamed column: https://northamptonpd.com/images/ODP%20Spreadsheets/2021/Use%20of%20Force.xlsx
@pytest.mark.parametrize('src, url, multitable', [
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/922", True), # Multiple separated tables in same sheet for different years
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/771", False), # 1st row is just the year and data type
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/882", False), # 1st row is just the year and data type
    ("Norwich", "https://www.norwichct.org/ArchiveCenter/ViewFile/Item/923", False), # 1st row is just the year and data type
    ("Northampton", "https://northamptonpd.com/images/ODP%20Spreadsheets/2021/Use%20of%20Force.xlsx", False), # 1st row is just the year and data type
    ('Omaha', 'https://cdn.muckrock.com/outbound_request_attachments/OmahaPoliceDepartment/87672/OIS202010-2019202.xlsx', True)
])
def test_1st_row_not_headers(skip, src, url, multitable):
    if src in skip:
        return

    loader = data_loaders.Excel(url)
    df = loader.load(pbar=False)

    def clean_df(df):
        df.columns= [x for x in df.iloc[0]]
        df = df.drop(index=df.index[0])
        if multitable:
            keep = df.apply(lambda x: not all([y==df.columns[k] for k,y in enumerate(x)]), axis=1)
            keep = keep & df.apply(lambda x: not x.iloc[2:].isnull().all(), axis=1)
            df = df[keep]

        df = df.reset_index(drop=True)
        df = df.convert_dtypes()
        df.columns = [x.strip() if isinstance(x, str) else x for x in df.columns]

        df = df.dropna(axis=0, thresh=2).reset_index(drop=True)

        return df
    
    headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}
    r = requests.get(url, stream=True, headers=headers)
    df_comp = pd.read_excel(BytesIO(r.content))
    if src=='Omaha':
        reps = [7, 2, 1]
        first_col_row = list(df_comp.loc[5])
        ct = 0
        rep_idx = 0
        for k, x in enumerate(first_col_row):
            if pd.notnull(x):
                ct = 1
            elif ct:
                ct+=1
                first_col_row[k] = first_col_row[k-1]
                if ct==reps[rep_idx]:
                    ct = 0
                    rep_idx+=1
    
        df_comp.columns = [x+" "+y if pd.notnull(x) else y for x,y in zip(first_col_row, list(df_comp.loc[6]))]
        df_comp = df_comp.loc[7:]
        df_comp = df_comp[df_comp['VICTIM/SUSPECT GANG AFFIL'].notnull() | df_comp['OFFICER NAME'].notnull() | df_comp['OFFICER SERGEANT'].notnull()]
        df_comp = df_comp[~df_comp['OFFICER SERGEANT'].isin(['OFFICER', 'SERGEANT'])]
        df_comp = df_comp.reset_index(drop=True).convert_dtypes()
    else:
        df_comp = clean_df(df_comp)

    pd.testing.assert_frame_equal(df, df_comp)


@pytest.mark.parametrize('src, url, date_field, yrs', [
    ("Northampton", "https://northamptonpd.com/images/ODP%20Spreadsheets/2014-2020_MV_Pursuits_incident_level_data.xlsx", "Date", range(2014,2021)), # This dataset has a typo in 1 of the year sheet names
    ("Northampton", "https://northamptonpd.com/images/ODP%20Spreadsheets/NPD_Use_of_Force_2014-2020_incident_level_data.xlsx", "Year", range(2014,2021)), # This dataset has a typo in the column names of some sheets
    ('Louisville', 'https://www.arcgis.com/sharing/rest/content/items/73672aa470da4095a88fcac074ee00e6/data', 'Year', range(2011, 2022))
]
)
def test_excel_year_sheets(skip, src, url, date_field, yrs):
    if src in skip:
        return
    
    warnings.filterwarnings('ignore', message='Identified difference in column names', category=UserWarning)
    warnings.filterwarnings('ignore', message=r"Column '.+' in current DataFrame does not match '.+' in new DataFrame. When they are concatenated, both columns will be included.", category=UserWarning)

    loader = data_loaders.Excel(url, date_field=date_field)

    years = loader.get_years()
    yrs = [x for x in yrs]
    assert years == yrs

    def clean_df(df, yr):
        if all(['Unnamed' in x for x in df.columns[2:]]):
            df.columns= [x for x in df.iloc[0]]
            df = df.drop(index=df.index[0])
        elif any('Unnamed' in x for x in df.columns):
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
        df['Year'] = yr
        if 'Month' in df:
            cols = []
            for c in df.columns:
                if c=='Month':
                    cols.append('Year')
                    cols.append(c)
                elif c!='Year':
                    cols.append(c)
            df = df[cols]
        df = df.convert_dtypes()
        df.columns = [x.strip() if isinstance(x, str) else x for x in df.columns]
        if pd.isnull(df.columns[0]):
            df = df.iloc[:, 1:]
        return df
    
    df_comp = pd.read_excel(url, sheet_name=str(yrs[0]))
    df_comp = clean_df(df_comp, yrs[0])

    # Load all years
    df_loaded1 = loader.load(year=yrs[0], pbar=False)

    assert df_comp.equals(df_loaded1)

    df_comp = pd.read_excel(url, sheet_name=str(yrs[1]))
    df_comp = clean_df(df_comp, yrs[1])

    # Load all years
    df_loaded2 = loader.load(year=yrs[1], pbar=False)

    assert df_comp.equals(df_loaded2)

    df_multi = loader.load(year=[yrs[0]-1,yrs[1]], pbar=False)

    df_loaded2.columns = df_loaded1.columns  # This takes care of case where columns had typos which is handled by data loader
    assert df_multi.equals(pd.concat([df_loaded1, df_loaded2], ignore_index=True))

    df = loader.load(pbar=False)

    df2 = df[df_multi.columns].head(len(df_multi)).convert_dtypes()
    pd.testing.assert_frame_equal(df2, df_multi, check_dtype=False)

    df_last = loader.load(year=years[-1], pbar=False)
    if 'Incident/Type of Charges' in df_last:
        df_last = df_last.rename(columns={'Incident/Type of Charges':'Incident Type/Charges',
                                          'Event':'Event #',
                                          'Alcohol/Drugs':'Alcohol Drugs',
                                          'Arrest or ProtectiveCustody':'Arrest or Protective Custody'})
    elif 'Lethal Y/N' in df_last:
        df_last = df_last.rename(columns={'Lethal Y/N':'Lethal Y/YS/N'})
    df2 = df[df_last.columns].tail(len(df_last)).reset_index(drop=True).convert_dtypes()
    pd.testing.assert_frame_equal(df2, df_last, check_dtype=False)


def test_excel_header():
    url = "https://www.cityofsparks.us/2000-2024-SPD-OIS-Incidents%20(3).xlsx"

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
    df_comp = df_comp[[x for x in df_comp.columns if 'Unnamed' not in x]]
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