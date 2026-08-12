import json
import pytest
import re
import requests
import sys

import pandas as pd
try:
    import geopandas as gpd
    from shapely.geometry import Point
    _has_gpd = True
except:
    _has_gpd = False

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result


source = 'Philadelphia'
table = defs.TableType.CRASHES
url = 'phl'

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table) & (datasets['URL']==url)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def jsondata(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    url = "https://" + row['URL'] + ".carto.com/api/v2/sql"
    query = f"SELECT * FROM {row['dataset_id']} ORDER BY cartodb_id"
    r = requests.get(url, params={'q':query, 'format':'GeoJSON'})

    json = r.json()

    query = f"SELECT * FROM {row['dataset_id']} LIMIT 0"
    r = requests.get(url, params={'q':query, 'format':'JSON'})
    
    json['fields'] = r.json()['fields']
    
    return json


@pytest.fixture(scope='module')
def gt_pd(row, check_for_dataset, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    return pd.DataFrame.from_records([x['properties'] for x in jsondata['features']])


@pytest.fixture(scope='module')
def gt(check_for_dataset, gt_pd, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    df = gt_pd
    if _has_gpd:
        geometry = []
        for feat in jsondata["features"]:
            if "geometry" not in feat or feat["geometry"]==None or len(feat["geometry"]["coordinates"])<2:
                geometry.append(None)
            else:
                geometry.append(Point(feat["geometry"]["coordinates"][0], feat["geometry"]["coordinates"][1]))

        df = gpd.GeoDataFrame(df, crs=4326, geometry=geometry)

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Carto(url=row['URL'], date_field=row['date_field'], data_set=row['dataset_id'], query=row['query'])


def test_get_count(check_for_dataset, gt, loader):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count()
    assert count==len(gt)

@pytest.mark.parametrize('year', [2011, [2011, 2012]])
def test_get_count_year_filter(check_for_dataset, gt, loader, row, year):
    if not check_for_dataset(source, table):
        return
    
    year_list = [year] if not isinstance(year, list) else [y for y in range(year[0],year[1]+1)]
    is_year = gt[row['date_field']].isin(year_list)
    count_gt = is_year.sum()
    assert count_gt!=0, 'Ground truth count should not be 0'
    
    count = loader.get_count(year)
    assert count == count_gt


def test_get_count_date_filter(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2021-06-01','2022-06-01']
    with pytest.raises(ValueError, match='Count is not accurate'):
        loader.get_count(date)


@pytest.mark.parametrize('year', [2022, [2022, 2023]])
def test_count_cached(check_for_dataset, loader, year):
    if not check_for_dataset(source, table):
        return
    count = -42 # Actual query will never accidentally equal this number
    date = data_loaders.data_loader._clean_date_input(year)
    loader._last_count = (date, count, 'test')

    assert loader.get_count(year)==count


def test_count_not_cached(check_for_dataset, loader):
    if not check_for_dataset(source, table):
        return
    count = -42 # Actual query will never accidentally equal this number
    year = 2012
    date = data_loaders.data_loader._clean_date_input(year)
    where_query = loader._Carto__construct_where(date)
    loader._last_count = (date, count, where_query)

    assert loader.get_count(year-1)!=count


@pytest.mark.parametrize('date', [None, 2022, [2022, 2023]])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_year(check_for_dataset, gt, row, loader, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    if gt_date:
        gt_date = [pd.to_datetime(x, utc=True) for x in gt_date]
        gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))]

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


def test_pandas(check_for_dataset, gt_pd, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 5
    data_loaders.carto._use_gpd_force = False
    try:
        df = loader.load(nrows=nrows)
    except:
        raise
    finally:
        data_loaders.carto._use_gpd_force = None

    assert isinstance(df, pd.DataFrame)
    check_result(df.drop(columns='geolocation'), gt_pd.head(nrows), row)

def test_format_date_false(check_for_dataset, gt_pd, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 5
    data_loaders.carto._use_gpd_force = False
    try:
        df = loader.load(nrows=nrows, format_date=False)
    except:
        raise
    finally:
        data_loaders.carto._use_gpd_force = None

    assert isinstance(df, pd.DataFrame)
    check_result(df.drop(columns='geolocation'), gt_pd.head(nrows), row, convert_to_date=False)