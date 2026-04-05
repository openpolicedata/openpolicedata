import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_for_dataset, check_result

source = 'Connecticut'
table = defs.TableType.TRAFFIC
year = 2013
agency = 'Plymouth'

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
    results = client.get(row['dataset_id'], limit=200000,
                         where=f"{row['date_field']} >= '{year}-01-01' AND {row['date_field']} < '{year+1}-01-01'",
                         order=":id")

    df = pd.DataFrame.from_records(results)

    return df


@pytest.fixture(scope='module')
def gt(gt_raw, row):
    if not check_for_dataset(source, table):
        return None
    df = gt_raw.copy()
    df = df[df[row['agency_field']]==agency]
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])


def test_get_count_agency(gt, loader, row):
    if not check_for_dataset(source, table):
        return
    
    opt_filter = 'LOWER(' + row['agency_field'] + ") = '" + agency.lower() + "'"
    count = loader.get_count(year, opt_filter=opt_filter)

    is_year = gt[row['date_field']].dt.year==year
    assert count == is_year.sum()


def test_load_agency(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 10
    gt_date = data_loaders.data_loader._clean_date_input(year)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))]
    gt = gt[gt[row['agency_field']]==agency]
    gt = gt.head(nrows)
    
    opt_filter = 'LOWER(' + row['agency_field'] + ") = '" + agency.lower() + "'"
    df = loader.load(date=year, opt_filter=opt_filter, nrows=nrows)
    check_result(df, gt, row)

@pytest.mark.parametrize('partial_name', [None])
def test_get_agencies_all(gt_raw, row, loader, partial_name):
    if not check_for_dataset(source, table):
        return

    select = "DISTINCT " + row["agency_field"]

    agency_set = loader.load(year, select=select, output_type="set")
    truth = set(gt_raw[row['agency_field']].unique())
    assert agency_set==truth

@pytest.mark.parametrize('partial_name', [None])
def test_get_agencies_matching(gt_raw, row, loader, partial_name):
    if not check_for_dataset(source, table):
        return

    partial_name = 'Hartford'
    opt_filter = 'LOWER('+ row["agency_field"] + ") LIKE '%" + partial_name.lower() + "%'"
    select = "DISTINCT " + row["agency_field"]

    agency_set = loader.load(year, opt_filter=opt_filter, select=select, output_type="set")
    gt_raw = gt_raw[gt_raw[row['agency_field']].str.lower().str.contains(partial_name.lower())]
    truth = set(gt_raw[row['agency_field']].unique())
    assert agency_set==truth
