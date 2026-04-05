import pandas as pd
import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import defs, data_loaders, datetime_parser, Source
from test_utils import check_for_dataset, check_result

source = 'Jacksonville'
table = defs.TableType.SHOOTINGS

@pytest.fixture(scope='module')
def row(datasets):
    row = datasets[(datasets['SourceName']==source) & (datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]

@pytest.fixture(scope='module')
def gt_raw(row):
    if not check_for_dataset(source, table):
        return None
    
    df = pd.read_csv(row['URL'])

    return df


@pytest.fixture(scope='module')
def gt(gt_raw, row):
    if not check_for_dataset(source, table):
        return None
    df = gt_raw.copy()
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df

@pytest.fixture(scope='module')
def src():
    return Source(source)

# def get_count(self, 
#                   force: bool = False,
#                   verbose: bool | str | int = False,
#                   url: str | None = None,
#                   id: str | None = None
#                   ) -> int:

def test_get_count(gt, src):
    if not check_for_dataset(source, table):
        return
    
    assert len(gt)==src.get_count(table_type=table)


def test_get_count_year_date_filter_error(gt, src, row):
    if not check_for_dataset(source, table):
        return
    
    date = ['2024-04-01', '2025-11-01']
    with pytest.raises(ValueError):
        src.get_count(table, date)


def test_get_count_year_date_filter(gt, src, row):
    if not check_for_dataset(source, table):
        return
    
    date = ['2024-04-01', '2025-11-01']
    count = src.get_count(table, date, force=True)

    gt_date = data_loaders.data_loader._clean_date_input(date)
    test = (gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))
    assert count == test.sum()

# def load(self, 
#             verbose: bool | str | int = False,
#             format_date: bool = True,
#             url: str | None = None,
#             id: str | None = None
#             ) -> Table:


def test_load(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    date = ['2024-04-01', '2025-11-01']
    offset = 1
    nrows = 2
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date!='MULTIPLE' else gt

    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt
    
    df = src.load(table, date=date, nrows=nrows, offset=offset).table
    raise NotImplementedError('This currently fails due to bug in CSV loader. CSV loader needs fixed.')
    check_result(df, gt, row)