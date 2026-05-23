from calendar import monthrange
import pytest
import sys
from sodapy import Socrata as SocrataClient

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs
import pandas as pd

from test_utils import check_for_dataset, check_result

source = 'Bloomington'
table = defs.TableType.EMPLOYEE

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
    results = client.get(row['dataset_id'], limit=100000, order=":id")

    df = pd.DataFrame.from_records(results)
    df[row['date_field']] = df[row['date_field']].apply(lambda x: pd.Period(freq='Q', year=int(x[:4]), quarter=int(x[-1])))

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])


def test_get_count(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    year = 2017
    gt = gt[gt[row['date_field']].dt.year==year]
    
    count = loader.get_count(date=year)
    assert count==len(gt)


def test_load_year(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    year = 2017
    gt = gt[gt[row['date_field']].dt.year==year]
    
    df = loader.load(date=year)
    check_result(df, gt, row)


def test_load_date_range(loader):
    if not check_for_dataset(source, table):
        return
    
    with pytest.raises(ValueError, match='Unable to filter by date'):
        loader.load(date=['2017-03-31', '2017-07-01'])
