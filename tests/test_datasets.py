import urllib.error
from zipfile import ZipFile
import pandas as pd
import numpy as np
import os
import re
from packaging import version
import pytest
import urllib

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import openpolicedata as opd

import sys

local_csv_file = os.path.join("..",'opd-data','opd_source_table.csv')

@pytest.mark.parametrize("file", [(), (local_csv_file,)])
def test_reload(file):
    orig = opd.datasets.datasets
    opd.datasets.datasets = None
    assert opd.datasets.datasets is None
    try:
        if len(file)>0:
            if not os.path.exists(os.path.dirname(local_csv_file)):
                return
            assert os.path.exists(*file)
        opd.datasets.reload(*file)
        assert isinstance(opd.datasets.datasets, pd.DataFrame)
    except:
        raise
    finally:
        opd.datasets.datasets = orig


def test_duplicates(all_datasets):
    all_datasets = all_datasets.copy()
    all_datasets['dataset_id'] = all_datasets['dataset_id'].apply(lambda x: str(x) if hasattr(x,'__iter__') else x)
    assert not all_datasets.duplicated(subset=['State', 'SourceName', 'Agency', 'TableType','Year','URL','dataset_id']).any()


def test_check_columns(datasets):
    columns = {
        'State' : pd.StringDtype(),
        'SourceName' : pd.StringDtype(),
        'Agency': pd.StringDtype(),
        'TableType': pd.StringDtype(),
        'Year': np.dtype("O"),
        'Description': pd.StringDtype(),
        'DataType': pd.StringDtype(),
        'URL': pd.StringDtype(),
        'date_field': pd.StringDtype(),
        'dataset_id': pd.StringDtype(),
        'agency_field': pd.StringDtype()
    }

    for key in columns.keys():
        assert key in datasets


def test_table_for_nulls(datasets):
    can_have_nulls = ["Description", "date_field", "dataset_id", "agency_field", "Year","readme","min_version",
                        "AgencyFull","source_url","coverage_start","coverage_end",'query',
                        'agency_originated', 'supplying_entity','py_min_version']

    for col in datasets.columns:
        if not col in can_have_nulls:
            assert pd.isnull(datasets[col]).sum() == 0


def test_check_state_names(datasets):
    all_states = [
        'Alabama', 'Alaska', 'American Samoa', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia',
        'Florida', 'Georgia', 'Guam', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
        'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
        'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio', 'Oklahoma', 'Oregon',
        'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virgin Islands',
        'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'MULTIPLE'
    ]

    assert len([x for x in datasets["State"] if x not in all_states]) == 0

def test_agency_names(datasets):
    # Agency names should either match source name or be MULTI
    rem = datasets["Agency"][datasets["Agency"] != datasets["SourceName"]]
    assert ((rem == opd.defs.MULTI) | (rem == opd.defs.NA)).all()

def test_year(datasets):
    # year should either be an int or MULTI or "None"
    rem = datasets["Year"][[type(x)!=int for x in datasets["Year"]]]
    assert ((rem == opd.defs.MULTI) | (rem == opd.defs.NA)).all()

def test_single_year_coverage(datasets):
    for k in range(len(datasets)):
        if not isinstance(datasets.iloc[k]['Year'], str):
            year = datasets.iloc[k]['Year']
            assert datasets.iloc[k]['coverage_start'].year==year
            assert datasets.iloc[k]['coverage_end'].year==year

def test_multi_year_coverage(datasets):
    ds = datasets[datasets['Year']==opd.defs.MULTI]
    ignore_exceptions = [('Bloomington','OFFICER-INVOLVED SHOOTINGS')]
    ds = ds[~(ds['SourceName'].isin([x[0] for x in ignore_exceptions]) & ds['TableType'].isin([x[1] for x in ignore_exceptions]))]
    assert (ds['coverage_start'].dt.year!=ds['coverage_end'].dt.year).all()

@pytest.mark.parametrize('source, partial_url', [('stanford','stanford'),('muckrock','muckrock'),
                                                 ('California Office of the Attorney General', 'openjustice.doj.ca.gov'),
                                                 ('openpolicedata','opd-datasets')])
def test_3rd_party_source(datasets, source, partial_url):
    ds = datasets[datasets['URL'].str.lower().str.contains(partial_url)]
    assert (ds['agency_originated'].str.lower()=='yes').all()
    assert (ds['supplying_entity'].str.lower().str.contains(source.lower())).all()

@pytest.mark.parametrize('source, partial_url', [('stanford','stanford'),('muckrock','muckrock'),
                                                 ('California Office of the Attorney General', 'openjustice.doj.ca.gov'),
                                                 ('openpolicedata','opd-datasets')])
def test_not_3rd_party(datasets, source, partial_url):
    ds = datasets[~datasets['URL'].str.contains(partial_url)]
    assert not (ds['supplying_entity'].str.lower().str.contains(source.lower())).any()

def test_agency_multiple(datasets):
    ds = datasets[datasets['Agency']==opd.defs.MULTI]
    assert ds['supplying_entity'].notnull().all()
    is_multiple_state = ds['State']==opd.defs.MULTI
    # The following are currently true
    assert (ds['agency_originated'][is_multiple_state]=='no').all()
    assert (ds['agency_originated'][~is_multiple_state]=='yes').all()

@pytest.mark.parametrize('data_type', [opd.defs.DataType.SOCRATA, opd.defs.DataType.CARTO, opd.defs.DataType.CKAN])
def test_dataset_id(datasets, data_type):
    rem = datasets["dataset_id"][datasets["DataType"] == data_type]
    assert pd.isnull(rem).sum() == 0

def test_years_multi(datasets):
    # Multi-year datasets should typically have a value in date_field
    datasets = datasets[datasets["Year"] == opd.defs.MULTI]
    df_null = datasets[pd.isnull(datasets["date_field"])]
    
    # This can only be allowed for certain Excel cases
    assert df_null["DataType"].isin([opd.defs.DataType.EXCEL.value, opd.defs.DataType.CSV.value]).all()

def test_agencies_multi(datasets):
    rem = datasets["agency_field"][datasets["Agency"] == opd.defs.MULTI]
    assert rem.notnull().all()

def test_agencies_not_multi(datasets):
    rem = datasets["agency_field"][(datasets["Agency"] != opd.defs.MULTI) & (~datasets['source_url'].str.contains('openjustice.doj.ca.gov',na=False))]
    assert rem.isnull().all()

def test_zip(datasets):
    ds = datasets[datasets['URL'].str.endswith('zip')]
    urls = ds['URL'].unique()
    for url in urls:
        df = ds[ds['URL']==url]
        try:
            with opd.data_loaders.UrlIoContextManager(url) as fp:
                with ZipFile(fp, 'r') as z:
                    for k in range(len(df)):
                        if hasattr(df.iloc[k]['dataset_id'],'__iter__'):
                            if isinstance(df.iloc[k]['dataset_id'], str):
                                ids = [df.iloc[k]['dataset_id']]
                            elif isinstance(df.iloc[k]['dataset_id'],dict) and 'files' in df.iloc[k]['dataset_id']:
                                ids = df.iloc[k]['dataset_id']['files']
                                ids = [ids] if isinstance(ids, str) else ids
                            else:
                                raise NotImplementedError()
                            for id in ids:
                                assert id in z.namelist()

                                if df.iloc[k]['Agency']==opd.defs.MULTI and \
                                    'data-openjustice.doj.ca.gov' in df.iloc[k]['URL']:
                                    # Source should be county name
                                    assert df.iloc[k]['SourceName'].endswith(' County')
                                    county_name = df.iloc[k]['SourceName'].replace(' County','')
                                    assert county_name.lower() in id.lower()
                        else:
                            assert len(z.namelist())==1
        except urllib.error.HTTPError as e:
            assert df.iloc[0]['SourceName']=='Chicago'
            assert sys.version_info<=(3,10)
        except:
            raise

def test_arcgis_urls(datasets):
    urls = datasets["URL"]
    p = re.compile(r"(MapServer|FeatureServer)/\d+")
    for i,url in enumerate(urls):
        if datasets.iloc[i]["DataType"] == opd.defs.DataType.ArcGIS.value:
            result = p.search(url)
            assert result != None
            assert len(url) == result.span()[1]

def test_source_list_by_state(datasets, use_changed_rows):
    state = "Virginia"
    df = opd.datasets.query(state=state)
    df_truth = datasets[datasets["State"]==state]
    assert use_changed_rows or len(df)>0
    pd.testing.assert_frame_equal(df_truth, df)

def test_source_list_by_source_name(datasets, use_changed_rows):
    source_name = "Fairfax County"
    df = opd.datasets.query(source_name=source_name)
    df_truth = datasets[datasets["SourceName"]==source_name]
    assert use_changed_rows or len(df)>0
    pd.testing.assert_frame_equal(df_truth, df)

def test_source_list_by_agency(datasets, use_changed_rows):
    agency = "Fairfax County"
    df = opd.datasets.query(agency=agency)
    df_truth = datasets[datasets["Agency"]==agency]
    assert use_changed_rows or len(df)>0
    pd.testing.assert_frame_equal(df_truth, df)

def test_source_list_by_table_type(datasets, use_changed_rows):
    table_type = opd.defs.TableType.TRAFFIC
    df = opd.datasets.query(table_type=table_type)
    df_truth = datasets[datasets["TableType"]==table_type.value]
    assert use_changed_rows or len(df)>0
    pd.testing.assert_frame_equal(df_truth, df)

def test_source_list_by_table_type_value(datasets, use_changed_rows):
    table_type = opd.defs.TableType.TRAFFIC.value
    df = opd.datasets.query(table_type=table_type)
    df_truth = datasets[datasets["TableType"]==table_type]
    assert use_changed_rows or len(df)>0
    pd.testing.assert_frame_equal(df_truth, df)

def test_source_list_by_multi(datasets, use_changed_rows):
    state = "Virginia"
    source_name = "Fairfax County"
    table_type = opd.defs.TableType.TRAFFIC_CITATIONS.value
    df = opd.datasets.query(state=state, table_type=table_type, source_name=source_name)
    df_truth = datasets[datasets["TableType"]==table_type]
    df_truth = df_truth[df_truth["State"]==state]
    df_truth = df_truth[df_truth["SourceName"]==source_name]
    assert use_changed_rows or len(df)>0
    pd.testing.assert_frame_equal(df_truth, df)

def test_table_types(datasets):
    for t in datasets["TableType"]:
        # Try to convert to an enum
        opd.defs.TableType(t)

def test_data_types(datasets):
    for t in datasets["DataType"].unique():
        # Try to convert to an enum
        opd.defs.DataType(t)

def test_min_versions(datasets):
    for ver in datasets["min_version"][datasets["min_version"].notnull()]:
        if not (ver == "-1" or type(version.parse(ver)) == version.Version):
            raise ValueError(f"{ver} is an invalid value for min_version")
        
@pytest.mark.parametrize('data_type, ver_added', [('Excel','0.3.1'), ('Carto','0.4.1'), ('CKAN','0.6'), ('HTML','0.7.3')])
def test_loader_min_version(datasets, data_type, ver_added):
    datasets = datasets[datasets['DataType'].str.lower()==data_type.lower()]
    assert datasets['min_version'].isnull().sum()==0
    assert datasets['min_version'].apply(lambda x: x=='-1' or version.parse(x)>=version.parse(ver_added)).all()

def test_zip_multifile_min_version(datasets):
    datasets = datasets[datasets['URL'].str.endswith('.zip')]
    datasets = datasets[datasets['dataset_id'].notnull()]
    assert datasets['min_version'].isnull().sum()==0
    assert datasets['min_version'].apply(lambda x: x=='-1' or version.parse(x)>=version.parse('0.8.2')).all()
        
def test_summary_functions():
    opd.datasets.num_unique()
    opd.datasets.num_sources()
    opd.datasets.num_sources(full_states_only=True)
    opd.datasets.summary_by_state()
    opd.datasets.summary_by_state(by="YEAR")
    opd.datasets.summary_by_state(by="TABLE")
    opd.datasets.summary_by_table_type()
    opd.datasets.summary_by_table_type(by_year=True)

    opd.datasets.datasets.loc[0,"TableType"] = "TEST"
    with pytest.warns(UserWarning):
        opd.datasets.summary_by_table_type()

def test_get_table_types(all_datasets):   # Passing in all_datasets to ensure that local changes are used if csvfile is set 
    opd.datasets.get_table_types()
    stops_tables = opd.datasets.get_table_types(contains="STOPS")
    exp_stops_tables = all_datasets['TableType'][all_datasets['TableType'].str.contains('STOPS')].unique()
    assert len(stops_tables) == len(exp_stops_tables)
    assert all([x in exp_stops_tables for x in stops_tables])

def test_combined_num_datasets(all_datasets):
    for k in range(len(all_datasets)):
        if hasattr(all_datasets.iloc[k]['dataset_id'],'__iter__') and not isinstance(all_datasets.iloc[k]['dataset_id'],str):
            ds = opd.dataset_id.expand(all_datasets.iloc[k]['dataset_id'])

            if 'data-openjustice' in all_datasets.iloc[k]['URL'] and all_datasets.iloc[k]['Year']==2018:
                assert len(ds)==2 # Last 2 quarters of year
            elif all_datasets.iloc[k]['SourceName']=='Wallkill' and all_datasets.iloc[k]['Year']==2016:
                assert len(ds)==3
            else:
                # Data quality checks (not set in stone. may need to adjust for new cases)
                # Biannually, Quarterly, or monthly
                assert len(ds) in [1, 2, 4, 12]
            
            for m in range(len(ds)):
                if 'sheets' in ds[m]:
                    assert len(ds[m]['sheets']) == len(set(ds[m]['sheets']))
                for n in range(m+1, len(ds)):
                    assert ds[m]!=ds[n]


if __name__ == "__main__":
    csvfile = None
    csvfile = os.path.join('..', 'opd-data', "opd_source_table.csv")
    test_dataset_id(csvfile,None,None,None,None)
    # test_get_table_types(csvfile,None,None,None,None)
    # test_table_for_nulls(csvfile,None,None,None,None)
    # test_years_multi(csvfile,None,None,None,None)
    # test_table_types(csvfile,None,None,None,None)
    # test_agencies_multi(csvfile,None,None,None,None)
    # test_reload(csvfile,None,None,None,None, (csv_file,))