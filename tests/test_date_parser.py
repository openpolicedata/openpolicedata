import pytest

import numbers
import pandas as pd

import openpolicedata as opd

@pytest.fixture(scope="module")
def dates():
    src = opd.Source('Richmond')
    t = src.load('OFFICER-INVOLVED SHOOTINGS','MULTIPLE')
    return t.table[t.date_field]

@pytest.mark.parametrize('start,stop', [('2025-01-01', '2025-12-30'),('2025-01-02', '2025-12-31')])
def test_split_date_range_partial_year(start, stop):
    start = pd.Timestamp(start)
    stop = pd.Timestamp(stop)
    start_range, full_years, stop_range = opd.datetime_parser.split_date_range(start, stop)
    assert start_range==[start,stop]
    assert full_years==[]
    assert stop_range==None

@pytest.mark.parametrize('start,stop', [('2025-01-01', '2025-12-31'),('2023-01-01', '2025-12-31')])
def test_split_date_range_full_year(start, stop):
    start = pd.Timestamp(start)
    stop = pd.Timestamp(stop)
    start_range, full_years, stop_range = opd.datetime_parser.split_date_range(start, stop)
    assert start_range==None
    assert full_years==[x for x in range(start.year, stop.year+1)]
    assert stop_range==None

def test_split_date_range_partial_first_year():
    start = pd.Timestamp('2023-01-02')
    stop = pd.Timestamp('2025-12-31')
    start_range, full_years, stop_range = opd.datetime_parser.split_date_range(start, stop)
    assert start_range==[start, pd.Timestamp(year=start.year+1,month=1,day=1)-pd.Timedelta(seconds=1)]
    assert full_years==[x for x in range(start.year+1, stop.year+1)]
    assert stop_range==None

def test_split_date_range_partial_last_year():
    start = pd.Timestamp('2023-01-01')
    stop = pd.Timestamp('2025-12-30')
    start_range, full_years, stop_range = opd.datetime_parser.split_date_range(start, stop)
    assert start_range==None
    assert full_years==[x for x in range(start.year, stop.year)]
    assert stop_range==[pd.Timestamp(year=stop.year,month=1,day=1), stop]

def test_split_date_range_partial_first_and_last_year():
    start = pd.Timestamp('2023-01-02')
    stop = pd.Timestamp('2025-12-30')
    start_range, full_years, stop_range = opd.datetime_parser.split_date_range(start, stop)
    assert start_range==[start, pd.Timestamp(year=start.year+1,month=1,day=1)-pd.Timedelta(seconds=1)]
    assert full_years==[x for x in range(start.year+1, stop.year)]
    assert stop_range==[pd.Timestamp(year=stop.year,month=1,day=1), stop]


@pytest.mark.parametrize('format', ['%Y%m%d', '%Y%m%d.0', '%#m%d%Y.0', '%#m%d%Y', '%B %#d, %Y', '%#m/%#d/%y', '%#m/%#d/%Y',
                                    '%m-%d-%Y', '%Y-%m-%d', '%m/%d/%Y  00:00', '%Y-%m-%d 00:00:00+00', '%#m/%d%Y'])
def test_dates_to_datetime(dates, format):
    new_dates = dates.dt.strftime(format)
    dates_conv = opd.datetime_parser.to_datetime(new_dates)
    pd.testing.assert_series_equal(pd.to_datetime(dates.dt.date), dates_conv.dt.tz_localize(None), check_dtype=False)

@pytest.mark.parametrize('format', ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.000', '%Y-%m-%dT%H:%M:%S.000Z',
                                    '%m/%d/%Y %H%M hours', '%Y%m%d%H%M%S'])
def test_datetimes_to_datetime(dates, format):
    new_dates = dates.dt.strftime(format)
    dates_conv = opd.datetime_parser.to_datetime(new_dates)
    pd.testing.assert_series_equal(dates, dates_conv.dt.tz_localize(None))

def test_ymd_to_datetime(dates):
    dates_conv = opd.datetime_parser.to_datetime({'year':dates.dt.year,'month':dates.dt.month, 'day':dates.dt.day})
    dates_conv.name = dates.name
    pd.testing.assert_series_equal(pd.to_datetime(dates.dt.date), dates_conv, check_dtype=False)

@pytest.mark.parametrize('ordinal',[False, True])
@pytest.mark.parametrize('object',[False, True])
def test_ymd_ordinal_to_datetime(dates, object, ordinal):
    ymd = {'year':dates.dt.year.copy(),'month':dates.dt.month.copy(), 'day':dates.dt.day.copy()}
    dates_only = pd.to_datetime(dates.dt.date).astype('object')
    if ordinal:
        ymd['day'] = ymd['day'].astype('object')
        ords = ['st','nd','rd','th','th']  # Added extra th so number of ords and null cases are not equal
        for k in range(len(dates)):
            if pd.notnull(ymd['day'][k]):
                ymd['day'][k] = str(ymd['day'][k]) + ords[k%len(ords)]
                
    for k in range(len(dates)):
        for m in range(k%4):
            ymd[list(ymd.keys())[2-m]][k] = pd.NA

        if k%4==1:
            dates_only[k] = pd.Period(dates_only[k],'M')
        elif k%4==2:
            dates_only[k] = pd.Period(dates_only[k],'Y')
        elif k%4==3:
            dates_only[k] = pd.NaT

    df = pd.DataFrame(ymd)
    if object:
        df['day'] = df['day'].astype('object')
        df.loc[df['day'].isnull(),'day'] = pd.NA
    dates_conv = opd.datetime_parser.to_datetime(df)
    dates_conv.name = dates.name
    pd.testing.assert_series_equal(dates_only, dates_conv)

def test_unix_to_datetime(dates):
    unix = dates.apply(lambda x: x.timestamp())*1000
    dates_conv = opd.datetime_parser.to_datetime(unix, unit='ms')
    pd.testing.assert_series_equal(dates, dates_conv, check_dtype=False)

@pytest.mark.parametrize('format, period', [('%Y-%m-__', 'M'),('%Y-__-__', 'Y'),('%Y-__-%d', 'Y')]) 
def test_blank_to_datetime(dates, format, period):
    periods = dates.dt.to_period(freq=period)
    new_dates = dates.dt.strftime(format)
    dates_conv = opd.datetime_parser.to_datetime(new_dates, ignore_errors=True)
    pd.testing.assert_series_equal(periods, dates_conv)

@pytest.mark.parametrize('format',['%Y-%m-%d %H:%M:%S','%Y-%m-%dT%H:%M:%S.000Z'])
def test_mixed_to_datetime(dates, format):
    dates = dates.copy()

    new_dates = pd.Series({k:v.strftime('%Y-%m-%d') if k/2%1!=0 else v.strftime(format) for k,v in dates.items()}, name=dates.name)
    
    idx = dates.index/2 % 1!=0
    dates[idx] = dates[idx].dt.date
    
    dates_conv = opd.datetime_parser.to_datetime(new_dates)
    pd.testing.assert_series_equal(dates, dates_conv)

def test_mixed_floats_to_datetime(dates):
    new_dates = pd.Series({k:v.strftime('%Y%m%d.0') if k/2%1!=0 else v.strftime('%#m%d%Y.0') for k,v in dates.items()}, name=dates.name)
    dates_conv = opd.datetime_parser.to_datetime(new_dates)
    pd.testing.assert_series_equal(pd.to_datetime(dates.dt.date), dates_conv.dt.tz_localize(None), check_dtype=False)