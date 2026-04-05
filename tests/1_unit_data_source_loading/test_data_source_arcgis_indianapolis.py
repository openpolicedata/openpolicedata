import pandas as pd
import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_for_dataset, check_result

source = 'Indianapolis'
table = defs.TableType.SHOOTINGS
year = 2025

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def gt(row):
    if not check_for_dataset(source, table):
        return None
    
    loader = data_loaders.Arcgis(row['URL'], date_field=row['date_field'])
    df = loader.load(year)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def src():
    return Source(source)


def test_get_count(gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table, date=year)


@pytest.mark.parametrize('date', [year, [f'{year}-01-01', '2025-06-01']])
def test_get_count_year_date_filter(gt, src, row, date):
    if not check_for_dataset(source, table):
        return
    
    count = src.get_count(table, date)

    gt_date = data_loaders.data_loader._clean_date_input(date)
    test = (gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))
    assert count == test.sum()


def test_load_year_all(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    df = src.load(table, date=year).table
    check_result(df, gt, row)


@pytest.mark.parametrize('date', [year, [f'{year}-01-01', '2025-06-01']])
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


def test_format_date_false(src):
	if check_for_dataset(source, table):
		table = src.load(table, year, format_date=False, nrows=1)
		# Confirm date has not been formatted
		assert isinstance(table.table[table.date_field].iloc[0],str)