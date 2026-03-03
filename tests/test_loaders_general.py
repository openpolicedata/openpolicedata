import math

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

@pytest.fixture(scope='module')
def df_date():
    dt = pd.to_datetime('2026-01-01')
    d = {'date':[dt+pd.Timedelta(k*7, unit='D') for k in range(52)]}
    return pd.DataFrame(d)


def test_split_batches_lt_limit():
    nrows_req = [round(data_loaders.data_loader._default_limit/2), round(data_loaders.data_loader._default_limit/4)]
    batch_sizes, num_batches = data_loaders.data_loader._split_batches(nrows_req)

    assert batch_sizes==nrows_req
    assert num_batches==[1 for _ in nrows_req]

@pytest.mark.parametrize('nrows_req', [[data_loaders.data_loader._default_limit, data_loaders.data_loader._default_limit+1],
                         [data_loaders.data_loader._default_limit*2, round(data_loaders.data_loader._default_limit/4)]])
def test_split_batches(nrows_req):
    batch_sizes_gt = [x if x < data_loaders.data_loader._default_limit else data_loaders.data_loader._default_limit for x in nrows_req]
    num_batches_gt = [math.ceil(x / y) for x,y in zip(nrows_req, batch_sizes_gt)]

    batch_sizes, num_batches = data_loaders.data_loader._split_batches(nrows_req)

    assert batch_sizes==batch_sizes_gt
    assert num_batches==num_batches_gt

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


@pytest.mark.parametrize('offset', [None, 0])
@pytest.mark.parametrize('nrows', [None, int(1e6)])
def test_filter_inaccurate_date_query(df_date, offset, nrows):
    start = df_date['date'].min() + pd.Timedelta(1, unit='D')
    stop = df_date['date'].max() - pd.Timedelta(1, unit='D')

    df_date = df_date.copy()
    df_date.loc[df_date['date'].idxmin(), 'date'] = df_date.loc[df_date['date'].idxmin(), 'date'] + pd.Timedelta(1, unit='D') - pd.Timedelta(1, unit='ms')

    df = data_loaders.data_loader._filter_inaccurate_date_query(df_date, 'date', [start,stop], format_date=True, offset=offset, nrows=nrows)

    df_date_filt = df_date[(df_date['date']>=start) & (df_date['date'] < stop+pd.Timedelta(1, unit='D'))].reset_index(drop=True)
    pd.testing.assert_frame_equal(df, df_date_filt)


@pytest.mark.parametrize('offset', [None, 1])
@pytest.mark.parametrize('nrows', [None, 5])
def test_filter_inaccurate_date_query_segment(df_date, offset, nrows):
    start = df_date['date'].min() + pd.Timedelta(1, unit='D')
    stop = df_date['date'].max() - pd.Timedelta(1, unit='D')

    df_date = df_date.copy()
    df_date.loc[df_date['date'].idxmin(), 'date'] = df_date.loc[df_date['date'].idxmin(), 'date'] + pd.Timedelta(1, unit='D') - pd.Timedelta(1, unit='ms')

    df = data_loaders.data_loader._filter_inaccurate_date_query(df_date, 'date', [start,stop], format_date=True, offset=offset, nrows=nrows)

    truth = df_date[(df_date['date']>=start) & (df_date['date']<=stop)]
    if offset:
        truth = truth.iloc[offset:]
    if nrows!=None:
        truth = truth.head(nrows)

    truth = truth.reset_index(drop=True)

    assert df.equals(truth)

@pytest.mark.parametrize('offset, nrows', [[0, None], [1, None], [0,10], [1,9], [1,20]])
def test_setup_records_request_1_where_all(offset, nrows):
    date_field = None
    where = '1'
    count = 10
    accurate = True
    where = [data_loaders.data_loader.Where(where=where, count=count, accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==1
    assert nrows_req==[count-offset]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset


@pytest.mark.parametrize('offset, nrows', [[0, None], [1, None], [0,10], [1,9], [1,20]])
def test_setup_records_request_2_where_1empty(offset, nrows):
    date_field = None
    where_query = '1'
    count = 10
    accurate = True
    where = [data_loaders.data_loader.Where(where=where_query, count=count, accurate=accurate),
             data_loaders.data_loader.Where(where=where_query+'_2', count=0, accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==1
    assert where[0].where==where_query
    assert nrows_req==[count-offset]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset


def test_setup_records_request_1_where_partial():
    offset = 1
    count = 10
    nrows = 5
    date_field = None
    where = '1'
    accurate = True
    where = [data_loaders.data_loader.Where(where=where, count=count, accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==1
    assert nrows_req==[nrows]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset


def test_setup_records_request_1_where_not_accurate():
    offset = 1
    count = 10
    nrows = 5
    date_field = None
    where = '1'
    accurate = False
    where = [data_loaders.data_loader.Where(where=where, count=count, accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==1
    assert nrows_req==[count-offset]
    assert nrows_after_read==nrows
    assert offset_after_read==0
    assert new_offset==offset

@pytest.mark.parametrize('accurate', [[True, False], [False, True]])
def test_setup_records_request_2_where_not_accurate(accurate):
    offset = 1
    count = [10,20]
    nrows = 5
    date_field = 'date_field'
    where = [data_loaders.data_loader.Where(where='1', count=count[0], accurate=accurate[0]), \
             data_loaders.data_loader.Where(where='2', count=count[1], accurate=accurate[1])]
    nwhere = len(where)
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==nwhere
    assert nrows_req==count
    assert nrows_after_read==nrows
    assert offset_after_read==offset
    assert new_offset==0


@pytest.mark.parametrize('offset, nrows', [[0, 10], [1,9]])
def test_setup_records_request_3where_request_from_1(offset, nrows):
    count = [10,20,30]
    date_field = 'date_field'
    accurate = True
    where1 = '1'
    where = [data_loaders.data_loader.Where(where=where1, count=count[0], accurate=accurate), \
             data_loaders.data_loader.Where(where='2', count=count[1], accurate=accurate), \
             data_loaders.data_loader.Where(where='3', count=count[2], accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==1
    assert where[0].where==where1
    assert nrows_req==[nrows]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset


@pytest.mark.parametrize('offset, nrows', [[0, 30], [9,2]])
def test_setup_records_request_3where_overlap2_request(offset, nrows):
    count = [10,20,30]
    date_field = 'date_field'
    accurate = True
    where1,where2 = '1','2'
    where = [data_loaders.data_loader.Where(where=where1, count=count[0], accurate=accurate), \
             data_loaders.data_loader.Where(where=where2, count=count[1], accurate=accurate), \
             data_loaders.data_loader.Where(where='3', count=count[2], accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==2
    assert where[0].where==where1
    assert where[1].where==where2
    assert nrows_req==[count[0]-offset, nrows-(count[0]-offset)]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset


@pytest.mark.parametrize('offset, nrows', [[0, 60], [9,22], [9,500]])
def test_setup_records_request_3where_overlap3_request(offset, nrows):
    count = [10,20,30]
    date_field = 'date_field'
    accurate = True
    where1,where2,where3 = '1','2','3'
    where = [data_loaders.data_loader.Where(where=where1, count=count[0], accurate=accurate), \
             data_loaders.data_loader.Where(where=where2, count=count[1], accurate=accurate), \
             data_loaders.data_loader.Where(where=where3, count=count[2], accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==3
    assert nrows_req==[count[0]-offset, count[1], min(count[2],nrows-(count[0]-offset+count[1]))]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset


@pytest.mark.parametrize('offset, nrows', [[10, 50], [29,2], [29,500]])
def test_setup_records_request_3where_overlap2_skip1(offset, nrows):
    count = [10,20,30]
    date_field = 'date_field'
    accurate = True
    where1,where2,where3 = '1','2','3'
    where = [data_loaders.data_loader.Where(where=where1, count=count[0], accurate=accurate), \
             data_loaders.data_loader.Where(where=where2, count=count[1], accurate=accurate), \
             data_loaders.data_loader.Where(where=where3, count=count[2], accurate=accurate)]
    where, nrows_req, nrows_after_read, offset_after_read, new_offset = data_loaders.data_loader._setup_records_request(where, nrows, offset, None, date_field)

    assert len(where)==2
    assert where[0].where==where2
    assert where[1].where==where3
    assert nrows_req==[count[0]+count[1]-offset, min(count[2],nrows-(count[0]-offset+count[1]))]
    assert nrows_after_read==None
    assert offset_after_read==None
    assert new_offset==offset-count[0]


def test_last_count_update_accurate():
    last_count_in = None
    filter = ['2022-01-01T23:59:59','2022-02-27T01:00:00']
    filter = [pd.to_datetime(x) for x in filter]
    where = [data_loaders.data_loader.Where(where='1', count=10, accurate=True), \
             data_loaders.data_loader.Where(where='2', count=10, accurate=True)]
    opt_filter = ''
    
    last_count_out = data_loaders.data_loader._update_last_count(last_count_in, filter, where, opt_filter)
    assert last_count_out==((filter, opt_filter, where), where)

def test_last_count_update_inaccurate():
    last_count_in = None
    filter = ['2022-01-01T23:59:59','2022-02-27T01:00:00']
    filter = [pd.to_datetime(x) for x in filter]
    where = [data_loaders.data_loader.Where(where='1', count=10, accurate=True), \
             data_loaders.data_loader.Where(where='2', count=10, accurate=False)]
    opt_filter = ''
    
    last_count_out = data_loaders.data_loader._update_last_count(last_count_in, filter, where, opt_filter)
    assert last_count_out==last_count_in


@pytest.mark.parametrize('opt_filter', ['test', None])
def test_check_query_match_last_True(opt_filter):
    filter = ['2022-01-01T23:59:59','2022-02-27T01:00:00']
    filter = [pd.to_datetime(x) for x in filter]
    where = [data_loaders.data_loader.Where(where='1', count=10, accurate=True), \
             data_loaders.data_loader.Where(where='2', count=10, accurate=True)]
    
    last_count_old = data_loaders.data_loader._update_last_count(None, filter, where, opt_filter)

    where = [data_loaders.data_loader.Where(where='1', count=10, accurate=True), \
             data_loaders.data_loader.Where(where='2', count=10, accurate=True)]

    assert data_loaders.data_loader._check_query_match_last(last_count_old, filter, where, opt_filter)


@pytest.mark.parametrize('date1, where1, opt_filter2', [('2022-01-01T23:59:59', '1', 'test'),
                                                       ('2022-01-01T23:59:59', '3', None),
                                                       ('2021-01-01T23:59:59', '1', None)])
def test_check_query_match_last_False(date1, where1, opt_filter2):
    filter = ['2022-01-01T23:59:59','2022-02-27T01:00:00']
    filter = [pd.to_datetime(x) for x in filter]
    where = [data_loaders.data_loader.Where(where='1', count=10, accurate=True), \
             data_loaders.data_loader.Where(where='2', count=10, accurate=True)]
    opt_filter = None
    
    last_count_old = data_loaders.data_loader._update_last_count(None, filter, where, opt_filter)

    filter = [date1,'2022-02-27T01:00:00']
    filter = [pd.to_datetime(x) for x in filter]
    where = [data_loaders.data_loader.Where(where=where1, count=10, accurate=True), \
             data_loaders.data_loader.Where(where='2', count=10, accurate=True)]

    assert not data_loaders.data_loader._check_query_match_last(last_count_old, filter, where, opt_filter2)