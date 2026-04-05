from calendar import monthrange
import warnings
import pytest
import sys
from sodapy import Socrata as SocrataClient
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders, defs, datetime_parser
import pandas as pd

from test_utils import check_for_dataset, check_result

source = 'Orlando'
table = defs.TableType.SHOOTINGS

nrows = 100

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
    results = client.get(row['dataset_id'], limit=nrows, order=":id")

    df = pd.DataFrame.from_records(results)
    df[row['date_field']] = datetime_parser.to_datetime(df[row['date_field']])

    return df


@pytest.fixture(scope='module')
def loader(row):
    return data_loaders.Socrata(url=row['URL'], data_set=row['dataset_id'], date_field=row['date_field'])


def to_gpd(df):
    lon = df['geocoded_column'].apply(lambda x: x['coordinates'][0] if pd.notnull(x) else x)
    lat = df['geocoded_column'].apply(lambda x: x['coordinates'][1] if pd.notnull(x) else x)
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(lon, lat), crs="EPSG:4326").drop(columns='geocoded_column')


def test_geopandas(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    
    if not _has_gpd:
        warnings.warn('Skipping test_geopandas. Geopandas not installed.')
        return
    
    gt = to_gpd(gt)
    df = loader.load(nrows=nrows)
    assert isinstance(df, gpd.GeoDataFrame)
    check_result(df, gt, row)

def test_pandas(gt, row, loader):
    if not check_for_dataset(source, table):
        return
    if not _has_gpd:
        warnings.warn('Skipping test_pandas. Geopandas not installed.')

    data_loaders.socrata._use_gpd_force = False
    try:
        df = loader.load(nrows=nrows)
    except:
        raise
    finally:
        data_loaders.socrata._use_gpd_force = None

    assert isinstance(df, pd.DataFrame)
    check_result(df, gt, row)


@pytest.mark.parametrize('date', [['2024-01-02', '2024-12-08'], ['2017-01-02', '2018-01-01'], ['2017-01-02', '2019-01-01']])
def test_load_date_range(gt, row, loader, date):
    if not check_for_dataset(source, table):
        return
    
    gt_date = data_loaders.data_loader._clean_date_input(date)
    gt = gt[(gt[row['date_field']]>=gt_date[0]) & (gt[row['date_field']]<gt_date[1]+pd.Timedelta(1, unit='D'))] if gt_date else gt

    if _has_gpd:
        gt = to_gpd(gt)
    
    df = loader.load(date=date)
    check_result(df, gt, row)
