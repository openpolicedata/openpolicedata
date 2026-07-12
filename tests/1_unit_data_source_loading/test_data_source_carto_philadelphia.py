import pandas as pd
import pytest

import openpolicedata
import openpolicedata.data_loaders

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_result

source = 'Philadelphia'
table = defs.TableType.SHOOTINGS

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]

@pytest.fixture(scope='module')
def gt(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    loader = openpolicedata.data_loaders.Carto(row['URL'], row['dataset_id'], row['date_field'], query={'officer_involved':'Y'})
    df = loader.load()
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']]).dt.tz_localize(None)

    return df

@pytest.fixture(scope='module')
def src():
    return Source(source)

def test_get_count(check_for_dataset, gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table)


@pytest.mark.parametrize('date', [2022, [2022, 2023], ['2023-04-01', '2024-11-01']])
def test_get_count_year_date_filter(check_for_dataset, gt, src, row, date):
    if not check_for_dataset(source, table):
        return
    
    count = src.get_count(table, date)

    gt_date = data_loaders.data_loader._clean_date_input(date)
    test = (gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))
    assert count == test.sum()


def test_load_year_all(check_for_dataset, gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    df = src.load(table, date='MULTIPLE').table
    check_result(df, gt, row)


@pytest.mark.parametrize('date', [2024, [2023, 2024], ['2023-04-01', '2024-11-01']])
@pytest.mark.parametrize('nrows', [None, 2])
@pytest.mark.parametrize('offset', [0, 1])
def test_load(check_for_dataset, gt, row, src, date, nrows, offset):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date!='MULTIPLE' else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    
    df = src.load(table, date=date, nrows=nrows, offset=offset).table
    check_result(df, gt, row)