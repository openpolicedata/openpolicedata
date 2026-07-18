from shapely.geometry import Point
import pytest
import sys
import re
import requests

try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result

source = 'Tacoma'
table = defs.TableType.SHOOTINGS

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def jsondata(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    p = re.search(r"(MapServer|FeatureServer)/\d+", row['URL'])
    url = row['URL'][:p.span()[1]]+'/query'
    r = requests.get(url, {'f':'json','where':'1=1','outFields':'*'})
    r.raise_for_status()
    
    return r.json()


@pytest.fixture(scope='module')
def gt_raw(check_for_dataset, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    return pd.DataFrame.from_records([x['attributes'] for x in jsondata['features']])


@pytest.fixture(scope='module')
def gt_pd(check_for_dataset, row, gt_raw, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    df = gt_raw.copy()
    date_cols = [x["name"] for x in jsondata['fields'] if x["type"]=='esriFieldTypeDate' and x['name'].lower()!='time']
    date_cols.append(row['date_field'])

    for d in date_cols:
        df[d] = datetime_parser.to_datetime(df[d], unit="ms")

    return df

@pytest.fixture(scope='module')
def gt(check_for_dataset, gt_pd, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    df = gt_pd
    if _has_gpd:
        geometry = []
        for feat in jsondata["features"]:
            geometry.append(Point(feat["geometry"]["x"], feat["geometry"]["y"]))

        df = gpd.GeoDataFrame(df, crs=2927, geometry=geometry)

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Arcgis(url=row['URL'], date_field=row['date_field'])


def test_notfile(loader):
    assert not loader.isfile()


def test_get_count(check_for_dataset, gt, loader):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count()
    assert count==len(gt)

@pytest.mark.parametrize('year', [2022, [2022, 2023]])
def test_get_count_year_filter(check_for_dataset, gt, loader, row, year):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count(year)

    year = [year] if not isinstance(year, list) else [y for y in range(year[0],year[1]+1)]
    is_year = gt[row['date_field']].dt.year.isin(year)
    assert count == is_year.sum()


def test_get_count_date_filter(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2021-06-01','2022-06-01']
    dts = [pd.to_datetime(x) for x in date]
    gt = gt[(gt[row['date_field']]>=dts[0]) & (gt[row['date_field']]<dts[1]+pd.Timedelta(days=1))]
    
    count = loader.get_count(date)
    assert count==len(gt)


@pytest.mark.parametrize('year', [2022, [2022, 2023]])
def test_count_cached(check_for_dataset, loader, year):
    if not check_for_dataset(source, table):
        return
    count = -42 # Actual query will never accidentally equal this number
    date = data_loaders.data_loader._clean_date_input(year)
    loader._last_count = ((date,None), count, 'test')

    assert loader.get_count(year)==count


@pytest.mark.parametrize('next_year, next_where', [(2013, None), (2012, r"date_time LIKE '%1900%'")])
def test_count_not_cached(check_for_dataset, loader, next_year, next_where):
    if not check_for_dataset(source, table):
        return
    count = -42 # Actual query will never accidentally equal this number
    year = 2012
    date = data_loaders.data_loader._clean_date_input(year)
    where_query = loader._Arcgis__construct_where(date)
    loader._last_count = ((date,None), count, where_query)

    if next_where:
        next_where = f"occurred_date >= '{year}-01-01' AND  occurred_date <= '{year}-12-31T23:59:59.999'"
    assert loader.get_count(next_year, where=next_where)!=count


@pytest.mark.parametrize('date', [None, 2022, [2022, 2023]])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_year(check_for_dataset, gt, row, loader, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt

    assert len(gt)>0
    
    df = loader.load(date=date, nrows=nrows, offset=offset)
    check_result(df, gt, row)


def test_load_count0_too_big_offset(check_for_dataset, loader):
    if not check_for_dataset(source, table):
        return
    df = loader.load(offset=10_000_000)  # Simulate with offset that is undoubtedly larger than dataset
    assert len(df)==0


def test_load_count0_date_out_of_range(check_for_dataset, row, loader):
    if not check_for_dataset(source, table):
        return
    date = [row['coverage_start']-pd.Timedelta(days=365*2), row['coverage_start']-pd.Timedelta(days=365)]
    df = loader.load(date=date)  # Simulate with offset that is undoubtedly larger than dataset
    assert len(df)==0


@pytest.mark.parametrize('date', [['2017-02-01', '2017-12-08'], ['2017-01-02', '2018-01-01'], ['2017-01-02', '2019-01-01']])
def test_load_date_range(check_for_dataset, gt, row, loader, date):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt
    
    df = loader.load(date=date)
    check_result(df, gt, row)


def test_get_count_with_where(check_for_dataset, gt, loader):
    if not check_for_dataset(source, table):
        return
    
    col = 'inside_outside'
    val = 'Outside'
    
    where = f"{col} = '{val}'"
    gt = gt[gt[col]==val]
    
    count = loader.get_count(where=where)
    assert len(gt)==count


def test_get_count_with_where_and_date(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    col = 'inside_outside'
    val = 'Outside'
    
    where = f"{col} = '{val}'"
    gt = gt[gt[col]==val]

    year = 2018
    gt_date = data_loaders.data_loader._clean_date_input(year)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt
    
    count = loader.get_count(date=year, where=where)
    assert len(gt)==count

def test_pandas(check_for_dataset, gt_pd, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 5
    data_loaders.arcgis_class._use_gpd_force = False
    try:
        df = loader.load(nrows=nrows)
    except:
        raise
    finally:
        data_loaders.arcgis_class._use_gpd_force = None

    assert isinstance(df, pd.DataFrame)
    check_result(df.drop(columns='geolocation'), gt_pd.head(nrows), row)

def test_format_date_false(check_for_dataset, gt_raw, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 5
    data_loaders.arcgis_class._use_gpd_force = False
    try:
        df = loader.load(nrows=nrows, format_date=False)
    except:
        raise
    finally:
        data_loaders.arcgis_class._use_gpd_force = None

    assert isinstance(df, pd.DataFrame)
    check_result(df.drop(columns='geolocation'), gt_raw.head(nrows), row, convert_to_date=False)