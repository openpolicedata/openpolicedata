import pytest
import requests
import sys

import pandas as pd

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result

source = 'Phoenix'
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
    count_gt = is_year.sum()
    assert count_gt!=0, 'Ground truth count should not be 0'
    assert count == is_year.sum()


def test_get_count_date_filter(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2021-06-01','2022-06-01']
    dts = [pd.to_datetime(x) for x in date]
    gt = gt[(gt[row['date_field']]>=dts[0]) & (gt[row['date_field']]<dts[1]+pd.Timedelta(days=1))]
    
    count = loader.get_count(date)
    assert len(gt)!=0, 'Ground truth count should not be 0'
    assert count==len(gt)


@pytest.mark.parametrize('year, opt_filter', [(1900, None), (1900,'test'), ([1900, 1901],'test')])
def test_count_cached(check_for_dataset, loader, year, opt_filter):
    if not check_for_dataset(source, table):
        return
    count = -42 # Actual query will never accidentally equal this number
    date = data_loaders.data_loader._clean_date_input(year)
    loader._last_count = ((date,opt_filter,'*'), count, None)

    assert loader.get_count(year, opt_filter=opt_filter)==count


@pytest.mark.parametrize('field', ['*', 'DATE'])
def test_count_cached_fields(check_for_dataset, loader, field):
    if not check_for_dataset(source, table):
        return

    year = 1900
    opt_filter = None
    count = -42 # Actual query will never accidentally equal this number
    date = data_loaders.data_loader._clean_date_input(year)
    loader._last_count = ((date,opt_filter,field), count, None)

    assert loader._Ckan__get_count(date, opt_filter, True, out_fields=field)[0]==count


@pytest.mark.parametrize('next_year, next_where, field', [(2013, None, '*'), (2012, r""""DATE" < '1980-01-01'""", '*'), (2012, None, 'DATE')])
def test_count_not_cached(check_for_dataset, loader, next_year, next_where, field):
    if not check_for_dataset(source, table):
        return
    count = -42 # Actual query will never accidentally equal this number
    year = 2012
    date = data_loaders.data_loader._clean_date_input(year)
    loader._last_count = ((date,None,field), count, None)

    assert loader.get_count(next_year, opt_filter=next_where)!=count


@pytest.mark.parametrize('date', [None, 2022, [2022, 2023]])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_year(check_for_dataset, gt, row, loader, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    if gt_date:
        gt_date = [pd.to_datetime(x) for x in gt_date]
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

def test_get_count_date_range_no_date_field(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return
    
    loader = data_loaders.Ckan(url=row['URL'], data_set=row['dataset_id'], query=row['query'])
    with pytest.raises(ValueError, match='has no date field'):
        loader.get_count(['2022-01-01',2023])


def test_load_date_range_no_date_field(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return
    
    loader = data_loaders.Ckan(url=row['URL'], data_set=row['dataset_id'], query=row['query'])
    with pytest.raises(ValueError, match='has no date field'):
        loader.load(['2022-01-01',2023])


def test_format_date_false(check_for_dataset, gt_raw, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 5
    df = loader.load(nrows=nrows, format_date=False)

    check_result(df, gt_raw.head(nrows), row, convert_to_date=False)