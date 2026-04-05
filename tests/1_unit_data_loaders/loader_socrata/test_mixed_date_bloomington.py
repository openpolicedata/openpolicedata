from calendar import monthrange
import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_for_dataset, check_result

source = 'Bloomington'
table = defs.TableType.CITATIONS

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
    results = client.get(row['dataset_id'], limit=100000, order=":id")

    df = pd.DataFrame.from_records(results)

    return df


@pytest.fixture(scope='module')
def gt(gt_raw, row):
    if not check_for_dataset(source, table):
        return None
    df = gt_raw.copy()
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])


def test_load_year(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    date = 2017
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt
    
    df = loader.load(date=date)
    check_result(df, gt, row)


@pytest.mark.parametrize('date', [['2017-06-01', '2017-07-08'], ['2017-01-02', '2018-01-01'], ['2017-01-02', '2019-01-01']])
def test_load_date_range(gt, row, loader, date):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt
    
    df = loader.load(date=date)
    check_result(df, gt, row)
