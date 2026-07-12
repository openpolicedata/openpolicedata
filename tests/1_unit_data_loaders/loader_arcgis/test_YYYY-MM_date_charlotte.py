import pytest
import sys
import re
import requests
from shapely.geometry import Point

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

source = 'Charlotte-Mecklenburg'
table = defs.TableType.SHOOTINGS_INCIDENTS

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
def gt(check_for_dataset, row, gt_raw, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    df = gt_raw.copy()
    date_cols = [x["name"] for x in jsondata['fields'] if x["type"]=='esriFieldTypeDate' and x['name'].lower()!='time']
    date_cols.append(row['date_field'])

    for d in date_cols:
        df[d] = datetime_parser.to_datetime(df[d], unit="ms")

    if _has_gpd:
        geometry = []
        for feat in jsondata["features"]:
            geometry.append(Point(feat["geometry"]["x"], feat["geometry"]["y"]))

        df = gpd.GeoDataFrame(df, crs=2927, geometry=geometry)

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Arcgis(url=row['URL'], date_field=row['date_field'])


@pytest.mark.parametrize('year', [2023, [2023,2024]])
def test_get_count_year_filter(check_for_dataset, gt, loader, row, year):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count(year)

    year = [year] if not isinstance(year, list) else [y for y in range(year[0],year[1]+1)]
    is_year = gt[row['date_field']].dt.year.isin(year)

    assert count==is_year.sum()
    assert count>0
        


def test_get_count_date_filter(check_for_dataset, loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2022-06-01','2023-06-01']    
    with pytest.raises(ValueError, match='Count is not accurate'):
        loader.get_count(date)


@pytest.mark.parametrize('date', [2024, [2023,2024]])
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
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']], unit="ms")
    check_result(df, gt, row)


def test_load_date_range(check_for_dataset, loader):
    if not check_for_dataset(source, table):
        return
     
    date = ['2022-06-01','2023-06-01']
    with pytest.raises(ValueError, match='Date field only provides'):
        loader.load(date)