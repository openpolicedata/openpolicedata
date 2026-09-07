import pytest
import requests
import sys

import pandas as pd

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result

source = 'Boston'
table = defs.TableType.INCIDENTS
year = 'MULTIPLE'
id = 'b973d8cb-eeb2-4e7e-99da-c92938efc9c0'

year_query = 2023
month_query = 2

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table) & (datasets['Year']==year) & (datasets['dataset_id'] == id)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def jsondata(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None

    url = row['URL']
    if url.startswith("https://"):
                url = url.replace("https://", "")
    if url.endswith('/'):
        url = url[:-1]

    where = '"' + row['date_field'] + '"' + rf" LIKE '%{year_query}-{month_query:02}%'"
    query = f'SELECT * FROM "{row['dataset_id']}" WHERE {where} ORDER BY "_id" OFFSET 0'
    params = {'format':'json', 'sql':query}
    url = "https://" + url + "/api/3/action/datastore_search_sql"
    r = requests.get(url, params=params)

    json = r.json()
    
    return json


@pytest.fixture(scope='module')
def gt_raw(check_for_dataset, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    df = pd.DataFrame(jsondata['result']['records'])
    return df[[x for x in df.columns if not x.startswith('_')]]


@pytest.fixture(scope='module')
def gt(check_for_dataset, gt_raw, jsondata):
    if not check_for_dataset(source, table):
        return None
    
    df = gt_raw.copy()
    date_cols = [x['id'] for x in jsondata['result']["fields"] if x["type"] in ['timestamp','date']]

    for d in date_cols:
        df[d] = datetime_parser.to_datetime(df[d], unit="ms")

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Ckan(url=row['URL'], date_field=row['date_field'], data_set=row['dataset_id'], query=row['query'])


def test_get_count_date_filter(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = [f'{year_query}-{month_query:02}-02',f'{year_query}-{month_query:02}-05']
    dts = [pd.to_datetime(x) for x in date]
    gt_dts = pd.to_datetime(gt[row['date_field']]).dt.tz_localize(None)
    gt = gt[(gt_dts>=dts[0]) & (gt_dts<dts[1]+pd.Timedelta(days=1))]
    
    count = loader.get_count(date)
    assert len(gt)!=0, 'Ground truth count should not be 0'
    assert count==len(gt)


def test_load_date_range(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return

    date = [f'{year_query}-{month_query:02}-02',f'{year_query}-{month_query:02}-05']
    dts = [pd.to_datetime(x) for x in date]
    gt_dts = pd.to_datetime(gt[row['date_field']]).dt.tz_localize(None)
    gt = gt[(gt_dts>=dts[0]) & (gt_dts<dts[1]+pd.Timedelta(days=1))]

    assert len(gt)>0
    
    df = loader.load(date=date, format_date=False)
    check_result(df, gt, row, convert_to_date=False)
