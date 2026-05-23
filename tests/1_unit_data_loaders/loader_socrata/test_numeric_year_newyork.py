import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_for_dataset, check_result

source = 'New York'
table = defs.TableType.TRAFFIC_CITATIONS
year = 2023
agency = 'VILLAGE OF AMITYVILLE  PD'

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
    results = client.get(row['dataset_id'], limit=200000,
                         where=f"{row['date_field']} >= {year} AND {row['date_field']} < {year+1} AND {row['agency_field']} = '{agency}'",
                         order=":id")

    df = pd.DataFrame.from_records(results)
    df[row['date_field']] = df[row['date_field']].apply(int)

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])


def test_get_count_agency(gt, loader, row):
    if not check_for_dataset(source, table):
        return
    
    opt_filter = 'LOWER(' + row['agency_field'] + ") = '" + agency.lower() + "'"
    count = loader.get_count(year, opt_filter=opt_filter)

    assert count == len(gt)

def test_load_agency(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    nrows = 10
    gt = gt.head(10)
    
    opt_filter = 'LOWER(' + row['agency_field'] + ") = '" + agency.lower() + "'"
    df = loader.load(date=year, opt_filter=opt_filter, nrows=nrows)
    df[row['date_field']] = df[row['date_field']].apply(int)
    check_result(df, gt, row, convert_to_date=False)

@pytest.mark.parametrize('date',[[f'{year}-01-01', f'{year}-12-30'],[f'{year}-01-02', f'{year}-12-31']])
def test_load_date_filter(loader, date):
    if not check_for_dataset(source, table):
        return
    
    with pytest.raises(ValueError):
        loader.load(date=date)
        