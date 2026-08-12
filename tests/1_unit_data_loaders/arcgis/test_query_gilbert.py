import json
import pytest
import sys
import re
import requests

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result

source = 'Gilbert'
table = defs.TableType.EMPLOYEE

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
    offset = 0
    features = []
    while True:
        r = requests.get(url, {'f':'json','where':'1=1','outFields':'*', 'resultOffset':offset})
        r.raise_for_status()
        j = r.json()
        if len(j['features'])==0:
            break

        offset += len(j['features'])
        features.extend(j['features'])
    
    return features


@pytest.fixture(scope='module')
def gt_raw(check_for_dataset, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    return pd.DataFrame.from_records([x['attributes'] for x in jsondata])


@pytest.fixture(scope='module')
def gt(check_for_dataset, gt_raw):
    if not check_for_dataset(source, table):
        return None
    
    return gt_raw[gt_raw['Department']=='POLICE DEPARTMENT']


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Arcgis(url=row['URL'], date_field=row['date_field'], query=row['query'])


def test_get_count(check_for_dataset, gt, loader):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count()
    assert len(gt)==count
        

@pytest.mark.parametrize('date', [2023, [2022,2024], ['2022-06-01','2023-06-01']])
def test_get_count_date_filter_error(check_for_dataset, loader, date):
    if not check_for_dataset(source, table):
        return
     
    with pytest.raises(ValueError, match='has no date field'):
        loader.get_count(date)


@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load(check_for_dataset, gt, row, loader, nrows, offset):
    if not check_for_dataset(source, table):
        return

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt

    assert len(gt)>0
    
    df = loader.load(nrows=nrows, offset=offset)
    check_result(df, gt, row)

@pytest.mark.parametrize('date', [2023, [2022,2024], ['2022-06-01','2023-06-01']])
def test_load_date_range(check_for_dataset, loader, date):
    if not check_for_dataset(source, table):
        return
     
    with pytest.raises(ValueError, match='has no date field'):
        loader.load(date)