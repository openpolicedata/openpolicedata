import pytest
import sys
import requests

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
import openpolicedata as opd
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result

source = 'Virginia'
table = defs.TableType.STOPS
year = 2020
agency = 'Ashland Police Department'

def get_url(row):
    url = row['URL']
    if url.startswith("https://"):
        url = url.replace("https://", "")
    if url.endswith('/'):
        url = url[:-1]
    url = "https://" + url + "/api/3/action/datastore_search_sql"

    return url

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def gt(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    url = get_url(row)

    where = f"""("{row['date_field']}" >= '{year}-01-01' AND "{row['date_field']}" <= '{year}-12-31T23:59:59.999')"""
    opt_filter = 'LOWER("' + row['agency_field'] + '"' + ") = '" + agency.lower() + "'"
    query = f'SELECT * FROM "{row['dataset_id']}" WHERE {where} AND {opt_filter} ORDER BY "_id" OFFSET 0'
    params = {'format':'json', 'sql':query}
    r = requests.get(url, params=params)

    json = r.json()

    df = pd.DataFrame(json['result']['records'])
    df = df[[x for x in df.columns if not x.startswith('_')]]

    date_cols = [x['id'] for x in json['result']["fields"] if x["type"] in ['timestamp','date']]

    for d in date_cols:
        df[d] = datetime_parser.to_datetime(df[d], unit="ms")

    return df

@pytest.fixture(scope='module')
def gt_agencies(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None

    query = 'SELECT DISTINCT "' + row["agency_field"] + f'" FROM "{row['dataset_id']}"'

    params = {'format':'json', 'sql':query}
    url = get_url(row)
    r = requests.get(url, params=params)

    json = r.json()

    return [x[row['agency_field']] for x in json['result']['records']]


def test_get_count_agency(check_for_dataset, gt):
    if not check_for_dataset(source, table):
        return
    
    src = opd.Source(source)
    count = src.get_count(table, year, agency)

    assert len(gt)>0
    assert count == len(gt)


@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load_agency(check_for_dataset, gt, row, offset, nrows):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(year)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))]
    gt = gt[gt[row['agency_field']]==agency]

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt

    assert len(gt)>0

    src = opd.Source(source)
    t = src.load(table, year, agency=agency, nrows=nrows, offset=offset)
    
    assert((t.table[row['agency_field']].str.lower()==agency.lower()).all())
    check_result(t.table, gt, row)

def test_get_agencies_all(check_for_dataset, gt_agencies):
    if not check_for_dataset(source, table):
        return

    src = opd.Source(source)
    agency_set = src.get_agencies(table)
    
    assert len(agency_set)==len(set(agency_set))
    assert set(agency_set)==set(gt_agencies)

def test_get_agencies_matching(check_for_dataset, gt_agencies):
    if not check_for_dataset(source, table):
        return

    partial_name = 'Arlington'
    src = opd.Source(source)
    agency_set = src.get_agencies(table, partial_name=partial_name)

    truth = [x for x in gt_agencies if partial_name in x]

    assert len(agency_set)==len(set(agency_set))
    assert all(partial_name.lower() in x.lower() for x in agency_set)
    assert set(agency_set)==set(truth)


def test_get_agencies_error_year_input(check_for_dataset):
    if not check_for_dataset(source, table):
        return

    src = opd.Source(source)
    with pytest.raises(ValueError):
        src.get_agencies(table, 2021)
