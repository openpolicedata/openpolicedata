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
table = defs.TableType.DEATHS_IN_CUSTODY

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
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
    
    query = f'SELECT * FROM "{row['dataset_id']}" ORDER BY "_id" OFFSET 0'
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


@pytest.mark.parametrize('year', [2016, [2016, 2017]])
def test_get_count_year_filter(check_for_dataset, gt, loader, row, year):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count(year)

    year = [year] if not isinstance(year, list) else [y for y in range(year[0],year[1]+1)]
    is_year = gt[row['date_field']].apply(lambda x: any(str(y) in x for y in year))
    count_gt = is_year.sum()
    assert count_gt!=0, 'Ground truth count should not be 0'
    assert count == is_year.sum()


def test_get_count_date_filter(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return

    with pytest.raises(ValueError, match='Count is not accurate for date input'):
        loader.get_count(['2016-06-01','2017-06-01'])


@pytest.mark.parametrize('date', [None, [2019, 2020]])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_year(check_for_dataset, gt, row, loader, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    if date:
        year = [date] if not isinstance(date, list) else [y for y in range(date[0],date[1]+1)]
        is_year = gt[row['date_field']].apply(lambda x: any(str(y) in x for y in year))
        gt = gt[is_year]

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt

    assert len(gt)>0
    
    df = loader.load(date=date, nrows=nrows, offset=offset)
    check_result(df, gt, row, convert_to_date=False)


@pytest.mark.parametrize('offset',[0,1])
def test_load_date_range(check_for_dataset, gt, row, loader, offset):
    if not check_for_dataset(source, table):
        return

    date = ['2019-06-01', '2020-01-05']
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    dts = pd.to_datetime(gt[row['date_field']])
    gt = gt[(dts>=gt_date[0]) & (dts<gt_date[1]+pd.Timedelta(1, unit='D'))]
    gt = gt.iloc[offset:]
    
    df = loader.load(date=date, offset=offset, format_date=False)
    check_result(df, gt, row, convert_to_date=False)
