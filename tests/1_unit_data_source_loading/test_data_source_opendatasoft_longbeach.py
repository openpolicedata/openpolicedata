import pandas as pd
import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_result

source = 'Long Beach'
table = defs.TableType.STOPS
year = 2019
url = 'longbeach'

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table) & (datasets['URL'].str.contains(url))]
    assert len(row)==1
    return row.iloc[0]

@pytest.fixture(scope='module')
def gt(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    loader = data_loaders.Opendatasoft(row['URL'], row['dataset_id'], row['date_field'])

    df = loader.load(year)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']]).dt.tz_localize(None)

    return df


@pytest.fixture(scope='module')
def src():
    return Source(source)


def test_get_count(check_for_dataset, gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table, date=year, url=url)


@pytest.mark.parametrize('date', [year, [f'{year}-04-01', f'{year}-11-01']])
def test_get_count_year_date_filter(check_for_dataset, gt, src, row, date):
    if not check_for_dataset(source, table):
        return
    
    count = src.get_count(table, date, url=url)

    gt_date = data_loaders.data_loader._clean_date_input(date)
    test = (gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))
    assert count == test.sum()


def test_load(check_for_dataset, row, src, gt):
    if not check_for_dataset(source, table):
        return
    
    date = [f'{year}-04-01', f'{year}-11-01']
    offset = 1
    nrows = 2

    df = src.load(table, date=date, nrows=nrows, offset=offset).table

    loader = data_loaders.Opendatasoft(row['URL'], row['dataset_id'], row['date_field'])
    gt = loader.load(date=date, nrows=nrows, offset=offset)
    gt[row['date_field']] = datetime_parser.to_datetime(gt[row['date_field']])

    check_result(df, gt, row)