from numpy.random import f
import pandas as pd
import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_result

source = 'Oakland'
table = defs.TableType.USE_OF_FORCE
year = 2012

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table) & (datasets['Year']==year)]
    assert len(row)==1
    return row.iloc[0]

@pytest.fixture(scope='module')
def gt(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    df = pd.read_excel(row['URL'])
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def src():
    return Source(source)

def test_get_count_error(check_for_dataset, src):
    if not check_for_dataset(source, table):
        return
    
    with pytest.raises(ValueError):
        src.get_count(table_type=table, date=year)

def test_get_count(check_for_dataset, gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table, date=year, force=True)

def test_get_count_year_date_filter(check_for_dataset, gt, src, row):
    if not check_for_dataset(source, table):
        return
    
    date = [f'{year}-04-01',f'{year}-06-01']
    
    count = src.get_count(table, date, force=True)

    gt_date = data_loaders.data_loader._clean_date_input(date)
    test = (gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))
    assert count == test.sum()

def test_load(check_for_dataset, gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    date = [f'{year}-04-01', f'{year}-11-01']
    offset = 1
    nrows = 2
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date!='MULTIPLE' else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    
    df = src.load(table, date=date, nrows=nrows, offset=offset).table
    check_result(df, gt, row)