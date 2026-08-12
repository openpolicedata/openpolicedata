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

source = 'Santa Rosa'
table = defs.TableType.INCIDENTS
year = 2019

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table) & (datasets['Year']==year)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def jsondata(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    p = re.search(r"(MapServer|FeatureServer)/\d+", row['URL'])
    url = row['URL'][:p.span()[1]]+'/query'
    offset = 0
    while True:
        r = requests.get(url, {'f':'json','where':'1=1','outFields':'*', 'resultOffset':offset, 'orderByFields': f'{row["date_field"]}, OBJECTID'})
        r.raise_for_status()
        j = r.json()
        if len(j['features'])==0:
            break

        if offset==0:
            jsondata = j
        else:
            jsondata['features'].extend(j['features'])

        offset += len(j['features'])
    
    return jsondata


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
            if 'geometry' in feat:
                geometry.append(Point(feat["geometry"]["x"], feat["geometry"]["y"]))
            else:
                geometry.append(None)

        df = gpd.GeoDataFrame(df, crs=2868, geometry=geometry)

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Arcgis(url=row['URL'], date_field=row['date_field'])


def test_get_count_date_filter(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2019-06-01','2019-12-03']
    dts = [pd.to_datetime(x) for x in date]
    gt = gt[(gt[row['date_field']]>=dts[0]) & (gt[row['date_field']]<dts[1]+pd.Timedelta(days=1))]
    
    count = loader.get_count(date)
    assert count==len(gt)


def test_load(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2019-06-01','2019-06-03']
    offset = 1
    nrows = 2
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt

    assert len(gt)>0
    
    df = loader.load(date=date, nrows=nrows, offset=offset)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']], unit="ms")
    check_result(df, gt, row)

