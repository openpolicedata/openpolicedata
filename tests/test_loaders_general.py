import pandas as pd
import pytest
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders

@pytest.fixture()
def df():
    d = {"col1": [1, 2,3,4,5], "FakeAgencyCol": ['Agency1','Agency2','Agency2','Agency1','Agency1'],
         'year':['2022', '2022','2021','2021','2021'], 'YearCol2':[2021, 2022, 2022, 2022, 2021],
         'date':['2022-01-01T00:00:00','2022-02-27T23:00:00','2022-02-28T00:00:00','2023-12-31T23:59:59','2023-11-01T00:00:00']}
    return pd.DataFrame(d)


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


def test_filter_dataframe_no_date_field(df):
    pd.testing.assert_frame_equal(df, data_loaders.data_loader._filter_dataframe(df.copy()))

def test_filter_dataframe_nan_date_field(df):
    pd.testing.assert_frame_equal(df, data_loaders.data_loader._filter_dataframe(df.copy(), date_field=pd.NA))

def test_filter_dataframe_no_format_date(df):
    pd.testing.assert_frame_equal(df, data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date', format_date=False))

def test_filter_dataframe_has_datefield(df):
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date')
    assert not df.equals(df2)
    df['date'] = pd.to_datetime(df['date'])
    pd.testing.assert_frame_equal(df, df2)

def test_filter_dataframe_has_datefield_baddata(df):
    df.loc[1,'date'] = 'BAD'
    pd.testing.assert_frame_equal(df, data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date'))

def test_filter_dataframe_has_datefield_baddata_filter(df):
    df.loc[1,'date'] = 'BAD'
    with pytest.raises(ValueError):
        data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date', date_filter=pd.to_datetime(['2023-01-01', '2023-12-31']))

def test_filter_dataframe_date_filter(df):
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date', date_filter=pd.to_datetime(['2023-01-01', '2023-12-31']))
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'].dt.year>=2023]
    pd.testing.assert_frame_equal(df, df2)

def test_filter_dataframe_datetime_filter(df):
    filter = ['2022-01-01T23:59:59','2022-02-27T01:00:00']
    filter = [pd.to_datetime(x) for x in filter]
    with pytest.warns(UserWarning):
        df2 = data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date', date_filter=filter)

    filter = [x.floor('24h') for x in filter]
    filter[1] = filter[1]+pd.Timedelta('1D')
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date']>=filter[0]) & (df['date']<filter[1])]
    pd.testing.assert_frame_equal(df, df2)


def test_filter_dataframe_filter_request_no_date_field(df):
    with pytest.raises(ValueError):
        data_loaders.data_loader._filter_dataframe(df.copy(), date_filter=pd.to_datetime(['2023-01-01', '2023-12-31']))


def test_filter_dataframe_filter_request_no_format_date(df):
    with pytest.raises(ValueError):
        data_loaders.data_loader._filter_dataframe(df.copy(), date_field='date', date_filter=pd.to_datetime(['2023-01-01', '2023-12-31']), format_date=False)


def test_filter_dataframe_year_string_column(df):
    year = 2022
    filter = [f'{year}-01-01',f'{year}-12-31']
    filter = [pd.to_datetime(x) for x in filter]
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), date_field='year', date_filter=filter)

    df = df[df['year']==f'{year}']
    pd.testing.assert_frame_equal(df, df2)


def test_filter_dataframe_year_numeric_column(df):
    year = 2022
    filter = [f'{year}-01-01',f'{year}-12-31']
    filter = [pd.to_datetime(x) for x in filter]
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), date_field='YearCol2', date_filter=filter)

    df = df[df['YearCol2']==year]
    pd.testing.assert_frame_equal(df, df2)


def test_filter_dataframe_year_multi_year_filter(df):
    year1 = 2022
    year2 = 2023
    filter = [f'{year1}-01-01',f'{year2}-12-31']
    filter = [pd.to_datetime(x) for x in filter]
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), date_field='YearCol2', date_filter=filter)

    df = df[df['YearCol2'].apply(lambda x: year1<=x<=year2)]
    pd.testing.assert_frame_equal(df, df2)


@pytest.mark.parametrize('filter', [['2023-01-01', '2023-12-29'], ['2023-01-02', '2023-12-31'],['2023-01-01', '2023-10-31'], ['2023-02-01', '2023-12-31']])
def test_filter_dataframe_year_nonyear_filter(df, filter):
    with pytest.raises(ValueError):
        data_loaders.data_loader._filter_dataframe(df.copy(), date_field='year', date_filter=pd.to_datetime(filter))


def test_filter_dataframe_agency_filter(df):
    agency_field='FakeAgencyCol'
    agency='Agency1'
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), agency_field=agency_field, agency=agency)

    df = df[df[agency_field]==agency]
    pd.testing.assert_frame_equal(df, df2)


def test_filter_dataframe_agency_and_date_filter(df):
    agency_field='FakeAgencyCol'
    agency='Agency1'
    df2 = data_loaders.data_loader._filter_dataframe(df.copy(), agency_field=agency_field, agency=agency, 
                                                     date_field='date', date_filter=pd.to_datetime(['2023-01-01', '2023-12-31']))

    df = df[df[agency_field]==agency]
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'].dt.year>=2023]
    pd.testing.assert_frame_equal(df, df2)


def test_filter_dataframe_agency_filter_nofield(df):
    with pytest.raises(ValueError):
        data_loaders.data_loader._filter_dataframe(df.copy(), agency='Agency1')