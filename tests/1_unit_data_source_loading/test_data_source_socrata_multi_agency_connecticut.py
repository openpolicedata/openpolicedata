import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
import openpolicedata as opd
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_for_dataset, check_result

source = 'Connecticut'
table = defs.TableType.TRAFFIC
year = 2018
agency = 'Plymouth'

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def gt(row):
    if not check_for_dataset(source, table):
        return None
    
    client = SocrataClient(row['URL'], data_loaders.socrata.default_sodapy_key, timeout=90)
    results = client.get(row['dataset_id'], limit=500000,
                         where=f"{row['date_field']} >= '{year}-01-01' AND {row['date_field']} < '{year+1}-01-01' and LOWER({row['agency_field']}) = '{agency.lower()}'",
                         order=":id")

    df = pd.DataFrame.from_records(results)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def gt_agencies(row):
    if not check_for_dataset(source, table):
        return None
    
    client = SocrataClient(row['URL'], data_loaders.socrata.default_sodapy_key, timeout=90)
    select = "DISTINCT " + row["agency_field"]
    results = client.get(row['dataset_id'], select=select)

    return [x['department_name'] for x in results if isinstance(x,dict) and len(x)>0]


def test_get_count_agency(gt):
    if not check_for_dataset(source, table):
        return
    
    src = opd.Source(source)
    count = src.get_count(table, year, agency)

    assert count == len(gt)


def test_load_agency(gt, row):
    if not check_for_dataset(source, table):
        return
    
    nrows = 10
    gt_date = data_loaders.data_loader._clean_date_input(year)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))]
    gt = gt[gt[row['agency_field']]==agency]
    gt = gt.head(nrows)

    src = opd.Source(source)
    t = src.load(table, year, agency=agency, nrows=nrows)
    
    assert((t.table[row['agency_field']].str.lower()==agency).all())
    check_result(t.table, gt, row)

def test_get_agencies_all(gt_agencies):
    if not check_for_dataset(source, table):
        return

    src = opd.Source(source)
    agency_set = src.get_agencies(table)
    
    assert len(agency_set)==set(agency_set)
    assert set(agency_set)==set(gt_agencies)

def test_get_agencies_matching(gt_agencies):
    if not check_for_dataset(source, table):
        return

    partial_name = 'Hartford'
    src = opd.Source(source)
    agency_set = src.get_agencies(table, partial_name=partial_name)

    truth = [x for x in gt_agencies if partial_name in x]

    assert len(agency_set)==set(agency_set)
    assert all(partial_name.lower() in x.lower() for x in agency_set)
    assert set(agency_set)==set(truth)


def test_get_agencies_error_year_input():
    if not check_for_dataset(source, table):
        return

    src = opd.Source(source)
    with pytest.raises(ValueError):
        src.get_agencies(table, 2021)
