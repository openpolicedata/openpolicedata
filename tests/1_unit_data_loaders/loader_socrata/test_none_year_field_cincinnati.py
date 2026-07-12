from calendar import monthrange
import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs
import pandas as pd

from test_utils import check_result

source = 'Cincinnati'
table = defs.TableType.SHOOTINGS_SUBJECTS

@pytest.fixture(scope='module')
def row(all_datasets):
    row = all_datasets[(all_datasets['SourceName']==source) & (all_datasets['TableType']==table)]
    assert len(row)==1
    return row.iloc[0]


@pytest.fixture(scope='module')
def gt(check_for_dataset, row):
    if not check_for_dataset(source, table):
        return None
    
    client = SocrataClient(row['URL'], data_loaders.socrata.default_sodapy_key, timeout=90)
    results = client.get(row['dataset_id'], limit=100000, order=":id")

    df = pd.DataFrame.from_records(results)

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])


def test_get_count(check_for_dataset, gt, loader):
    if not check_for_dataset(source, table):
        return
    
    count = loader.get_count()
    assert len(gt)==count
        

@pytest.mark.parametrize('date', [2023, [2022,2024], ['2022-06-01','2023-06-01']])
def test_get_count_date_filter_error(check_for_dataset, loader, date):
    if not check_for_dataset(source, table):
        return
     
    with pytest.raises(ValueError, match='has no date field'):
        loader.get_count(date)


@pytest.mark.parametrize('offset', [0, 1])
def test_load(check_for_dataset, gt, row, loader, offset):
    if not check_for_dataset(source, table):
        return

    nrows = 2
    gt = gt.iloc[offset:]
    gt = gt.head(nrows) if nrows else gt

    assert len(gt)>0
    
    df = loader.load(nrows=nrows, offset=offset)
    check_result(df, gt, row)

@pytest.mark.parametrize('date', [2023, [2022,2024], ['2022-06-01','2023-06-01']])
def test_load_date_range(check_for_dataset, loader, date):
    if not check_for_dataset(source, table):
        return
     
    with pytest.raises(ValueError, match='has no date field'):
        loader.load(date)