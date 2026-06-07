import pandas as pd
import pytest
import requests

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_for_dataset, check_result

source = 'Virginia'
table = defs.TableType.STOPS
year = 2020
agency = 'Ashland Police Department'

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]

@pytest.fixture(scope='module')
def gt_raw(row):
    if not check_for_dataset(source, table):
        return None
    
    opt_filter = 'LOWER("' + row['agency_field'] + '"' + ") = '" + agency.lower() + "'"
    
    loader = data_loaders.Ckan(row['URL'], row['dataset_id'], row['date_field'])
    df = loader.load(year, opt_filter=opt_filter)

    assert (df[row['date_field']].dt.year==year).all()

    return df


@pytest.fixture(scope='module')
def gt(gt_raw, row):
    if not check_for_dataset(source, table):
        return None
    df = gt_raw.copy()
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def src():
    return Source(source)


@pytest.fixture(scope='module')
def gt_agencies(row):
    if not check_for_dataset(source, table):
        return None
    
    url = 'https://data.virginia.gov/api/3/action/datastore_search_sql'
    params = {'format': 'json', 'sql': f'SELECT DISTINCT "{row["agency_field"]}" FROM "60506bbb-685f-4360-8a8c-30e137ce3615" OFFSET 0 LIMIT 10000'}
    r = requests.get(url, params=params)
    results = r.json()['result']['records']

    return [x[row["agency_field"]] for x in results if isinstance(x,dict) and len(x)>0]

def test_get_count_agency(gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table, date=year, agency=agency)

def test_load_agency(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    df = src.load(table, year, agency=agency).table
    check_result(df, gt, row)


@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_agency_subset(gt, row, src, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    date = [f'{year}-12-21', f'{year}-12-31']
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date!='MULTIPLE' else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    
    df = src.load(table, date=date, agency=agency, nrows=nrows, offset=offset).table
    check_result(df, gt, row)


def test_get_agencies_all(src, gt_agencies):
    if not check_for_dataset(source, table):
        return

    agency_set = src.get_agencies(table)
    
    assert len(agency_set)==set(agency_set)
    assert set(agency_set)==set(gt_agencies)

def test_get_agencies_matching(src, gt_agencies):
    if not check_for_dataset(source, table):
        return

    partial_name = 'Fairfax'
    agency_set = src.get_agencies(table, partial_name=partial_name)

    truth = [x for x in gt_agencies if partial_name in x]

    assert len(agency_set)==set(agency_set)
    assert all(partial_name.lower() in x.lower() for x in agency_set)
    assert set(agency_set)==set(truth)