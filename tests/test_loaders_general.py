import pandas as pd
import pytest
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders


def test_process_date_input_empty():
    with pytest.raises(ValueError):
        data_loaders.data_loader._process_date([])
    
def test_process_date_too_many():
    year = [2021, 2022, 2023]
    with pytest.raises(ValueError):
        data_loaders.data_loader._process_date(year)

def test_process_dates_year_input_wrong_order():
    year = [2023, 2021]
    with pytest.raises(ValueError):
        data_loaders.data_loader._process_date(year)

@pytest.mark.parametrize('loader_class, url, dataset, date_field', [
     (data_loaders.Socrata, "www.transparentrichmond.org","asfd-zcvn", "occurreddatetime"),
     (data_loaders.Ckan, 'https://data.boston.gov/', '58ad5180-f5f5-4893-a681-742971f71582', 'incident_date')])
def test_sortby_date_has_datefield(loader_class, url, dataset, date_field):
    loader = loader_class(url, dataset, date_field=date_field)
    df1 = loader.load(pbar=False, sortby='date')
    df2 = loader.load(pbar=False, sortby=date_field)

    pd.testing.assert_frame_equal(df1, df2)
    assert df1[date_field].sort_values().tolist()==df1[date_field].tolist()
     

@pytest.mark.parametrize('loader_class, url, dataset, date_field', [
     (data_loaders.Socrata, "www.transparentrichmond.org","asfd-zcvn", "occurreddatetime"),
     (data_loaders.Ckan, 'https://data.boston.gov/', '58ad5180-f5f5-4893-a681-742971f71582', 'incident_date')])
def test_sortby_no_datefield(loader_class, url, dataset, date_field):
    loader = loader_class(url, dataset, date_field=date_field)
    df1 = loader.load(pbar=False, sortby='date')
    df2 = loader.load(pbar=False)

    assert set(df1.columns) == set(df2.columns)

    df2 = df2[df1.columns]

    assert (df1[date_field] != df2[date_field]).any()  # Ensure sort is needed to match

    sort_cols = [date_field]
    sort_cols.extend([x for x in df1.columns if x!=date_field])
    pd.testing.assert_frame_equal(df1, df2.sort_values(sort_cols, ignore_index=True))


@pytest.mark.parametrize('loader_class, url, dataset, sort_col', [
     (data_loaders.Socrata, "www.transparentrichmond.org","asfd-zcvn", "officernumbershots"),
     (data_loaders.Ckan, 'https://data.boston.gov/', '58ad5180-f5f5-4893-a681-742971f71582', 'incident_district')])
def test_sortby_other_columns(loader_class, url, dataset, sort_col):
    loader = loader_class(url, dataset)
    df1 = loader.load(pbar=False, sortby=sort_col)

    # Ensure natural sorting
    col = df1[sort_col].apply(lambda x: int(x) if isinstance(x,str) and x.isdigit() else x)

    assert all((pd.isnull(x) and pd.isnull(y)) or x==y for x,y in zip(col.tolist(), col.sort_values().tolist()))