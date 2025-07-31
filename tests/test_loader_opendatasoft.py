import requests
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False


def test_opendatasoft():
    url = "data.longbeach.gov"
    dataset = "lbpd-ripa-data-annual"
    date_field = "stopdate"
    loader = data_loaders.Opendatasoft(url, dataset, date_field)

    assert not loader.isfile()

    count = loader.get_count()

    df_truth = pd.read_csv("https://data.longbeach.gov/api/explore/v2.1/catalog/datasets/lbpd-ripa-data-annual/exports/csv", 
                           delimiter=';', low_memory=False)
    assert count==len(df_truth)

    year = 2020
    count = loader.get_count(date=year)

    df_truth['stopdate'] = pd.to_datetime(df_truth['stopdate'])
    assert count==(df_truth['stopdate'].dt.year==year).sum()

    df = loader.load(date=year, pbar=False, sortby='date')

    assert len(df)==count

    offset = int(df[df['schoolname'].notnull()].index[0] if df['schoolname'].notnull().any() else 1)
    nrows = 100
    df_offset = loader.load(date=year, nrows=nrows, offset=offset, pbar=False, sortby='date')

    assert df_offset.convert_dtypes().equals(df.iloc[offset:offset+nrows].reset_index(drop=True).convert_dtypes())

    offset = count-2
    df_offset = loader.load(date=year, offset=offset, pbar=False, sortby='date')
    assert df_offset.convert_dtypes().equals(df.iloc[offset:].reset_index(drop=True).convert_dtypes())

    offset = min(count-2, 10000)
    nrows = 2
    df_offset = loader.load(date=year, offset=offset, nrows=nrows, pbar=False, sortby='date')
    assert df_offset.convert_dtypes().equals(df.iloc[offset:offset+nrows].reset_index(drop=True).convert_dtypes())

    df_truth = df_truth[df_truth['stopdate'].dt.year==year].sort_values(['stopdate','stopid','pid']).reset_index(drop=True)
    df['stopdate'] = pd.to_datetime(df['stopdate'])

    for c in df_truth.columns:
         if df_truth[c].dtype != df[c].dtype:
              df[c] = df[c].astype(df_truth[c].dtype)

    pd.testing.assert_frame_equal(df.sort_values(['stopdate','stopid','pid']).reset_index(drop=True), df_truth)
