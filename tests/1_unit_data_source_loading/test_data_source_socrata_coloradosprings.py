import os
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

@pytest.fixture(scope='module')
def whole_table(src):
    if not check_for_dataset(source, table):
        return
    
    return src.load(table, date='MULTIPLE')


def test_get_years(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    true_years = gt[row['date_field']].dt.year.unique()

    years = src.get_years(table)

    assert all(true_years==years)


def test_get_years_coverage_end_not_updated(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    true_years = gt[row['date_field']].dt.year.unique()

    # Note this will fail if this dataset ever falls behind because get_years only will check
    # future years if most recent year is within the last few years
    idx = src.datasets[src.datasets['TableType']==table].index
    # Simulate that coverage_end is old and more recent data available
    src.datasets.loc[idx, 'coverage_end'] = src.datasets.loc[idx, 'coverage_end'] - pd.Timedelta(days=366)

    years = src.get_years(table)

    assert all(true_years==years)


def test_get_years_use_coverage_only(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    true_years = gt[row['date_field']].dt.year.unique()

    # Note this will fail if this dataset ever falls behind because get_years only will check
    # future years if most recent year is within the last few years
    idx = src.datasets[src.datasets['TableType']==table].index
    # Simulate that coverage_end is old and more recent data available
    src.datasets.loc[idx, 'coverage_end'] = src.datasets.loc[idx, 'coverage_end'] - pd.Timedelta(days=366)

    years = src.get_years(table, use_coverage_only=True)

    true_years = list(true_years)
    true_years.remove(max(true_years))
    assert true_years==years


def test_get_years_manual(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    true_years = gt[row['date_field']].dt.year.unique()

    # Note this will fail if this dataset ever falls behind because get_years only will check
    # future years if most recent year is within the last few years
    idx = src.datasets[src.datasets['TableType']==table].index
    # Simulate that coverage_end is old and more recent data available
    src.datasets.loc[idx, 'coverage_end'] = src.datasets.loc[idx, 'coverage_end'] - pd.Timedelta(days=366)

    years = src.get_years(table, manual=True)

    assert all(true_years==years)

def test_get_years_req_years(gt, row, src):
    if not check_for_dataset(source, table):
        return
    
    true_years = gt[row['date_field']].dt.year.unique()

    req_years = true_years[:2]
    years = src.get_years(table, req_years=req_years)

    assert all(req_years==years)


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


def test_load_all(gt, row, src, whole_table):
    if not check_for_dataset(source, table):
        return
    
    check_result(whole_table.table, gt, row)


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


def test_load_gen(gt, row, src):
    if check_for_dataset(source, table):
        nbatch = 50
        df = pd.DataFrame()
        for t in src.load_iter(table, 'MULTIPLE', nbatch=nbatch):
            df = pd.concat([df, t.table])

        check_result(df, gt, row)

@pytest.mark.parametrize('save,fname,load',[('to_csv','get_csv_filename','load_csv'),
                                            ('to_feather','get_feather_filename','load_feather'),
                                            ('to_parquet','get_parquet_filename','load_parquet')])
def test_save_load(src, whole_table, save,fname,load):
    if check_for_dataset(source, table):

        getattr(whole_table, save)()

        filename = getattr(whole_table, fname)()
        assert os.path.exists(filename)

        try:
            # Load table back in
            getattr(src, load)(table_type=whole_table.table_type, date=whole_table.date, agency=whole_table.agency)
        except:
            raise
        finally:
            os.remove(filename)