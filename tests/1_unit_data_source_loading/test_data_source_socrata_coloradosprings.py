import pandas as pd
import pytest
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_for_dataset, check_result

source = 'Colorado Springs'
table = defs.TableType.SHOOTINGS

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
    results = client.get(row['dataset_id'],  limit=100000, order=":id")

    df = pd.DataFrame.from_records(results)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def src():
    return Source(source)

# def get_count(self, 
#                   verbose: bool | str | int = False,
#                   url: str | None = None,
#                   id: str | None = None
#                   ) -> int:

def test_get_count(gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table)


@pytest.mark.parametrize('date', [2022, [2022, 2023], ['2024-04-01', '2025-11-01']])
def test_get_count_year_date_filter(gt, src, row, date):
    if not check_for_dataset(source, table):
        return
    
    count = src.get_count(table, date)

    gt_date = data_loaders.data_loader._clean_date_input(date)
    test = (gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))
    assert count == test.sum()


def test_load_all(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    df = src.load(table, date='MULTIPLE').table
    check_result(df, gt, row)


def test_load_date_filter_zero_count(row, src):
    if not check_for_dataset(source, table):
        return
    
    date = [row['coverage_start']-pd.Timedelta(days=365*2), row['coverage_start']-pd.Timedelta(days=365)]
    df = src.load(table, date=date).table
    assert len(df)==0


@pytest.mark.parametrize('date', [2025, [2024, 2025], ['2024-04-01', '2025-11-01']])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load(gt, row, src, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date!='MULTIPLE' else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    
    df = src.load(table, date=date, nrows=nrows, offset=offset).table
    check_result(df, gt, row)