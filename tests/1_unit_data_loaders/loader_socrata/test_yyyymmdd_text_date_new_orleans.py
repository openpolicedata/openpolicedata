from calendar import monthrange
import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_result

source = 'New Orleans'
table = defs.TableType.COMPLAINTS

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def gt(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    client = SocrataClient(row['URL'], data_loaders.socrata.default_sodapy_key, timeout=90)
    results = client.get(row['dataset_id'], limit=100000, order=":id")

    df = pd.DataFrame.from_records(results)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])

def test_get_count_all(check_for_dataset, gt, loader):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==loader.get_count()

def test_get_count_year(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    year = 2024
    gt = gt[gt[row['date_field']].dt.year==year]
    
    count = loader.get_count(date=year)
    assert count==len(gt)


def test_load_year(check_for_dataset, gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    year = 2024
    gt = gt[gt[row['date_field']].dt.year==year]
    
    df = loader.load(date=year)
    check_result(df, gt, row)


@pytest.mark.parametrize('date', [['2024-06-01', '2024-07-08'], ['2024-01-02', '2025-01-01'], ['2023-01-02', '2025-01-01']])
def test_load_date_range(check_for_dataset, gt, row, loader, date):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt
    
    df = loader.load(date=date)
    check_result(df, gt, row)
