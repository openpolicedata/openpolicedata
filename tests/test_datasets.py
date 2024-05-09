import pandas as pd
import numpy as np
import os
import re
from packaging import version
import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import openpolicedata as opd

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import get_datasets


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
    assert not all_datasets.duplicated(subset=['State', 'SourceName', 'Agency', 'TableType','Year','source_url','dataset_id']).any()


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
                        "AgencyFull","source_url","coverage_start","coverage_end",'query']

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
        'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
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

@pytest.mark.parametrize('data_type', [opd.defs.DataType.SOCRATA, opd.defs.DataType.CARTO, opd.defs.DataType.CKAN])
def test_dataset_id(datasets, data_type):
    rem = datasets["dataset_id"][datasets["DataType"] == data_type]
    assert pd.isnull(rem).sum() == 0

def test_years_multi(datasets):
    # Multi-year datasets should typically have a value in date_field
    datasets = datasets[datasets["Year"] == opd.defs.MULTI]
    df_null = datasets[pd.isnull(datasets["date_field"])]
    
    # This can only be allowed for certain Excel cases
    assert (df_null["DataType"] == opd.defs.DataType.EXCEL.value).all()

def test_agencies_multi(datasets):
    rem = datasets["agency_field"][datasets["Agency"] == opd.defs.MULTI]
    assert pd.isnull(rem).sum() == 0

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
    exp_stops_tables = ["PEDESTRIAN STOPS","STOPS","TRAFFIC STOPS",
                        "TRAFFIC STOPS - INCIDENTS", 'TRAFFIC STOPS - SUBJECTS']
    assert len(stops_tables) == len(exp_stops_tables)
    assert all([x in exp_stops_tables for x in stops_tables])

def test_combined_num_datasets(all_datasets):
    for k in range(len(all_datasets)):
        if pd.notnull(all_datasets.iloc[k]['dataset_id']) and ';' in all_datasets.iloc[k]['dataset_id']:
            ds = all_datasets.iloc[k]['dataset_id']
            if '|' in ds:  # Before | are sheet names which we don't need
                ds = ds.split('|')[1]
            ds = [x.strip() for x in ds.split(';')]
            if all_datasets.iloc[k]['SourceName']=='Wallkill' and all_datasets.iloc[k]['Year']==2016:
                assert len(ds)==3
            else:
                # Data quality checks (not set in stone. may need to adjust for new cases)
                # Biannually, Quarterly, or monthly
                assert len(ds) in [2, 4,12]
            # No repeats
            assert len(ds) == len(set(ds))


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