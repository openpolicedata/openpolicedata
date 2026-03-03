from calendar import monthrange
import dataclasses
import math
import time
import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_for_dataset

source = 'Seattle'
table = defs.TableType.SHOOTINGS

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]

@pytest.fixture(scope='module')
def gt_raw(row):
    if not check_for_dataset(source, table):
        return None
    
    client = SocrataClient(row['URL'], data_loaders.socrata.default_sodapy_key, timeout=90)
    results = client.get(row['dataset_id'])

    df = pd.DataFrame.from_records(results)

    return df


@pytest.fixture(scope='module')
def gt(gt_raw, row):
    df = gt_raw.copy()
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])

def check_result(df, gt, row):
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    df = df.dropna(axis=1, how='all')
    assert gt.shape==df.shape
    df = df[gt.columns]  #Ensure columns are in correct order
    # Sorting is done to account for rows that are sorted by date but where rows with same date are in different order
    pd.testing.assert_frame_equal(df.sort_values(by=df.columns.tolist()).reset_index(drop=True), gt)
    time.sleep(0.1) # Just so we don't cause issues at the URL


def test_notfile(loader):
    assert not loader.isfile()


def test_get_count(gt, loader):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==loader.get_count()


def test_month_abbrev_test(loader):
    assert loader._Socrata__month_abbrev_test()

def test_mmddyyyy_test(loader):
    assert loader._Socrata__mmddyyyy_test()

@pytest.mark.parametrize('year', [2022, [2022, 2023]])
def test_get_count_year_filter(gt, loader, row, year):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count(year)

    year = [year] if not isinstance(year, list) else year
    is_year = gt[row['date_field']].dt.year.isin(year)
    assert count == is_year.sum()
    

def test_get_count_date_filter(loader):
    if not check_for_dataset(source, table):
        return
    
    date = ['2024-04-01', '2025-11-01']
    date = [pd.to_datetime(d) for d in date]

    with pytest.raises(ValueError, match='Count is not accurate for date input'):
        loader.get_count(date)


@pytest.mark.parametrize('year, opt_filter', [(1900, None), (1900,'test'), ([1900, 1901],'test')])
def test_count_cached(loader, year, opt_filter):
    count = -42 # Actual query will never accidentally equal this number
    date = data_loaders.data_loader._clean_date_input(year)
    where = loader._Socrata__construct_where(date, opt_filter)
    newwhere = [dataclasses.replace(where[0], count=count)]
    loader._last_count = data_loaders.data_loader._update_last_count(loader._last_count, date, newwhere, opt_filter)

    assert loader.get_count(year, opt_filter=opt_filter)==count


@pytest.mark.parametrize('next_year, next_opt_filter', [(1901, None), (1900, r"date_time LIKE '%1900%'")])
def test_count_not_cached(loader, next_year, next_opt_filter):
    count = -42 # Actual query will never accidentally equal this number
    year = 1900
    opt_filter = None
    date = data_loaders.data_loader._clean_date_input(year)
    where = loader._Socrata__construct_where(date, opt_filter)
    newwhere = [dataclasses.replace(where[0], count=count)]
    loader._last_count = data_loaders.data_loader._update_last_count(loader._last_count, date, newwhere, opt_filter)

    assert loader.get_count(next_year, opt_filter=next_opt_filter)!=count


@pytest.mark.parametrize('date', [None, 2025, [2024, 2025]])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load(gt, row, loader, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    gt = gt.dropna(axis=1, how='all')
    gt = gt.sort_values(by=gt.columns.tolist()).reset_index(drop=True)
    
    df = loader.load(date=date, nrows=nrows, offset=offset)
    check_result(df, gt, row)

def test_load_count0(loader):
    df = loader.load(offset=10_000_000)  # Simulate with offset that is undoubtedly larger than dataset
    assert len(df)==0


@pytest.mark.parametrize('date', [['2025-05-30','2025-10-30'], ['2005-03-01','2005-06-20'], ['2023-05-30','2025-10-30']])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_date_inaccurate_initial_filter_include(gt, row, loader, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    date = data_loaders.data_loader._clean_date_input(date)

    gt = gt[(gt[row['date_field']]>=date[0]) & (gt[row['date_field']]<date[1]+pd.Timedelta(1, unit='D'))] if date else gt
    
    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    gt = gt.dropna(axis=1, how='all')
    gt = gt.sort_values(by=gt.columns.tolist()).reset_index(drop=True)

    assert len(gt)>0
    
    df = loader.load(date=date, nrows=nrows, offset=offset)
    check_result(df, gt, row)


def test_load_date_inaccurate_initial_filter_exclude(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    gt = gt.sort_values(by=row['date_field'])
    maxdate = gt[row['date_field']][gt[row['date_field']].apply(lambda x: x.day>1)].iloc[-1]
    maxdate-=pd.Timedelta(days=1)
    maxdate=maxdate.floor('D')
    count = 0
    for x in gt[row['date_field']].sort_values(ascending=False):
        if x < maxdate:
            if count==0 or x.day==monthrange(x.year, x.month)[1]:
                count+=1
            else:
                mindate = (x + pd.Timedelta(days=1)).floor('D')
                break

    date = [mindate, maxdate+pd.Timedelta(days=1)-pd.Timedelta(seconds=1)]
    with pytest.warns(UserWarning, match='Times in date filter requests are ignored'):
        gtdate = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gtdate[0]) & (gt[row['date_field']]<gtdate[1]+pd.Timedelta(1, unit='D'))]

    assert len(gt)==count
    
    gt = gt.dropna(axis=1, how='all')
    gt = gt.sort_values(by=gt.columns.tolist()).reset_index(drop=True)
    
    with pytest.warns(UserWarning, match='Times in date filter requests are ignored'):
        df = loader.load(date=date)
    check_result(df, gt, row)


def test_load_batch(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    gt = gt.sort_values(by=row['date_field'])

    count = 3
    assert count>1
    date = [gt[row['date_field']].iloc[0].floor('D'), gt[row['date_field']].iloc[count-1].floor('D')]
    date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=date[0]) & (gt[row['date_field']]<date[1]+pd.Timedelta(1, unit='D'))]
    
    gt = gt.dropna(axis=1, how='all')
    gt = gt.sort_values(by=gt.columns.tolist()).reset_index(drop=True)
    
    orig = data_loaders.data_loader._default_limit
    data_loaders.data_loader._default_limit = math.ceil(count/2)
    try:
        df = loader.load(date=date)
    except:
        raise
    finally:
        data_loaders.data_loader._default_limit = orig

    check_result(df, gt, row)


def test_filter_MDDYYYY_vs_MMDDYYY():
    raise NotImplementedError()