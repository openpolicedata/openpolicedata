from datetime import datetime
from time import sleep
import warnings

import pandas as pd

if __name__ == "__main__":
    import sys
    sys.path.append('../openpolicedata')
import pytest

import openpolicedata as opd
from openpolicedata import defs, data

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_load_for_datasets, user_request_skip

@pytest.fixture(scope='module')
def api_datasets(remaining_datasets, is_api):
    keep = [x for x in remaining_datasets.index if x in is_api[is_api].index]
    return remaining_datasets.loc[keep]

@pytest.fixture(scope='module')
def no_datefield(api_datasets):
    return api_datasets['date_field'].isnull()


@pytest.fixture(scope='module')
def small_datasets(api_datasets, no_datefield):
    is_small = api_datasets['TableType'].str.contains('COMPLAINTS') | \
        api_datasets['TableType'].str.contains('SHOOTINGS') | \
        api_datasets['TableType'].str.contains('USE OF FORCE') | \
        api_datasets['TableType'].isin([defs.TableType.DISCIPLINARY_RECORDS, defs.TableType.EMPLOYEE, defs.TableType.DEATHS_IN_CUSTODY,
                                    defs.TableType.LAWSUITS, defs.TableType.POINTING_WEAPON, defs.TableType.VEHICLE_PURSUITS])
    return is_small & (~no_datefield)


@pytest.fixture(scope='module')
def large_datasets(no_datefield, small_datasets):
    return (~no_datefield) & (~small_datasets)

def test_all_datasets_tested(no_datefield, small_datasets, large_datasets):
    x = (no_datefield + small_datasets + large_datasets)!=1
    numleft = x.sum()
    assert numleft==0, f'{numleft} datasets not used or used more than once'


def test_load_no_date_field(api_datasets, no_datefield, source, start_idx, skip, query={}):
    check_load_for_datasets(api_datasets[no_datefield],  skip, start_idx, source, query, nrows=100)


def test_load_small_dataset_with_datefield(api_datasets, small_datasets, source, start_idx, skip, query={}):
    nrows = 10000
    def set_date(dataset, src, table_type):
        start = dataset['coverage_start'] + pd.Timedelta(days=1) if dataset['coverage_start']<dataset['coverage_end'] else dataset['coverage_start']
        if dataset['Year']==defs.MULTI and len(src.filter(table_type))==1:
            end = pd.Timestamp(datetime.now()).floor('D')
        else:
            end = dataset['coverage_end'] - pd.Timedelta(days=1) if dataset['coverage_start']<dataset['coverage_end'] else dataset['coverage_end']

        end = min(end, datetime.now().replace(hour=0,minute=0,second=0,microsecond=0))
        return [start, end]
    
    def check_date(dataset, table, date):
        assert len(table.table)<=nrows
        assert table.table[dataset['date_field']].min().tz_localize(None) >= date[0]
        assert table.table[dataset['date_field']].max().tz_localize(None) < date[1]+pd.Timedelta(days=1)
    
    check_load_for_datasets(api_datasets[small_datasets],  skip, start_idx, source, query, testfcn=check_date, datefcn=set_date, nrows=nrows)


def test_load_large_dataset_with_datefield(api_datasets, large_datasets, source, start_idx, skip, query={}):
    nrows = 10000
    def set_date(dataset, src, table_type):
        end = dataset['coverage_end'] - pd.Timedelta(days=2) if dataset['coverage_start']<dataset['coverage_end'] - pd.Timedelta(days=1) else dataset['coverage_end']
        end = min(end, datetime.now().replace(hour=0,minute=0,second=0,microsecond=0))
        start = max(dataset['coverage_start'], end - pd.Timedelta(days=30))
        return [start, end]
    
    def check_date(dataset, table, date):
        assert len(table.table)<=nrows
        if isinstance(table.table[dataset['date_field']].dtype, pd.PeriodDtype):
            assert table.table[dataset['date_field']].min().year==date[0].year
            assert table.table[dataset['date_field']].max().year==date[1].year
        else:
            assert table.table[dataset['date_field']].min().tz_localize(None) >= date[0]
            assert table.table[dataset['date_field']].max().tz_localize(None) < date[1]+pd.Timedelta(days=2) # Adding 2 days: 1 to not consider time of day and a 2nd for timezone offsets

    check_load_for_datasets(api_datasets[large_datasets],  skip, start_idx, source, query, datefcn=set_date, testfcn=check_date, nrows=nrows)


@pytest.mark.veryslow(reason="This is a very slow test that should be run before a major commit.")
def test_load_annual_dataset(api_datasets, source, start_idx, skip, query={}):
    nrows = 10000
    def set_date(dataset, src, table_type):
        return dataset['Year']
    
    def check_date(dataset, table, date):
        assert len(table.table)<=nrows
        if pd.notnull(dataset['date_field']):
            if table.table[dataset['date_field']].notnull().any():
                if isinstance(table.table[dataset['date_field']].dtype, pd.PeriodDtype):
                    assert table.table[dataset['date_field']].min().year==dataset['Year']
                    assert table.table[dataset['date_field']].max().year==dataset['Year']
                else:
                    if (dataset['SourceName'],dataset['TableType'], dataset['Year']) not in [('Santa Rosa','INCIDENTS', 2015),('Santa Rosa','INCIDENTS', 2016)]:
                        assert table.table[dataset['date_field']].min().tz_localize(None) >= pd.to_datetime(f"{dataset['Year']-1}-12-31")
                    if table.source_name=='Lincoln' and table.table_type==opd.defs.TableType.VEHICLE_PURSUITS and dataset['Year']==2019:
                        # This dataset is labeled 2019 and there is a separate dataset for 2020-2021. However, there is some 2020-2021 data in this
                        assert table.table[dataset['date_field']].max().tz_localize(None).floor('D') == pd.to_datetime(f"2021-02-21")
                    else:
                        assert table.table[dataset['date_field']].max().tz_localize(None) <= pd.to_datetime(f"{dataset['Year']+1}-01-02") # Adding 2 days: 1 to not consider time of day and a 2nd for timezone offsets
        
    annual_datasets = api_datasets[api_datasets['Year'].apply(lambda x: not isinstance(x,str))]
    check_load_for_datasets(annual_datasets,  skip, start_idx, source, query, datefcn=set_date, testfcn=check_date, nrows=nrows)


@pytest.mark.slow(reason="This test is slow to run and will be run last.")
def test_get_years(api_datasets, source, start_idx, skip, query={}):
    datasets = api_datasets
    cur_year = datetime.now().year

    caught_exceptions = []
    caught_exceptions_warn = []
    warn_errors = (opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError, opd.exceptions.OPD_FutureError)

    already_ran = []
    last_source = None
    for i in range(len(datasets)):
        if user_request_skip(datasets, i, skip, start_idx, source, query):
            continue

        srcName = datasets.iloc[i]["SourceName"]
        state = datasets.iloc[i]["State"]
        table = datasets.iloc[i]["TableType"]

        if (srcName, state, table) in already_ran or datasets.iloc[i]['Year']=='NONE':
            continue

        src = data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

        now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
        print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table} table")			

        already_ran.append((srcName, state, table))

        if srcName == last_source:
            sleep(num_sources*0.1) # Sleep for a bit to not hit the same site repeatedly too hard
            num_sources +=1
        else:
            num_sources = 1
        last_source = srcName

        mdatasets = src.filter(table)

        years_expected = set()
        for k in mdatasets.index:
            years_expected.update(range(mdatasets.loc[k,'coverage_start'].year, mdatasets.loc[k,'coverage_end'].year+1))

        most_recent = mdatasets['coverage_end'].idxmax()
        if mdatasets.loc[most_recent,'Year']==opd.defs.MULTI and mdatasets.loc[most_recent, 'coverage_end'].year > cur_year-2 and \
            (len(mdatasets)==1 or (mdatasets['coverage_end']>= mdatasets.loc[most_recent, 'coverage_end'] - pd.Timedelta(days=366)).sum()==1):
            # Reduce number of years in coverage to force get_years to request most recent years from the data source
            src.datasets.loc[most_recent, 'coverage_end'] = src.datasets.loc[most_recent, 'coverage_end'] - pd.Timedelta(days=366)

        try:
            years = src.get_years(table, force=True)
        except warn_errors as e:
            e.prepend(f"Iteration {i}", srcName, table)
            caught_exceptions_warn.append(e)
            continue
        except (opd.exceptions.OPD_TooManyRequestsError, opd.exceptions.OPD_arcgisAuthInfoError) as e:
            # Catch exceptions related to URLs not functioning
            e.prepend(f"Iteration {i}", srcName, table)
            caught_exceptions.append(e)
            continue
        except:
            raise

        if set(years)!=years_expected:
            t = src.load('COMPLAINTS - BACKGROUND',2026)
            if mdatasets.loc[most_recent,'Year']==opd.defs.MULTI:
                # Check if the dataset has any data right now. Perhaps it's not available
                count = src.get_count(table, opd.defs.MULTI, url=mdatasets.loc[most_recent,'URL'], id=mdatasets.loc[most_recent,'dataset_id'])
                if count>0:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()

        # if len(years)==0 and has_outages and \
        # 	(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
        # 	caught_exceptions_warn.append(f'Outage continues for {str(datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]])}')
        # 	continue

        # if datasets.iloc[i]["Year"] != MULTI:
        # 	assert datasets.iloc[i]["Year"] in years
        # else:
        # 	assert len(years) > 0

    if len(caught_exceptions)==1:
        raise caught_exceptions[0]
    elif len(caught_exceptions)>0:
        msg = f"{len(caught_exceptions)} URL errors encountered:\n"
        for e in caught_exceptions:
            msg += "\t" + e.args[0] + "\n"
        raise opd.exceptions.OPD_MultipleErrors(msg)

    for e in caught_exceptions_warn:
        warnings.warn(str(e))