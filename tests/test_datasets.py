import pytest
import pandas as pd
import numpy as np
import re

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import openpolicedata as opd


def get_datasets(csvfile):
    if csvfile != None:
        opd._datasets.datasets = opd._datasets._build(csvfile)

    return opd.datasets_query()


class TestDatasets:
    def test_duplicates(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        assert not datasets.duplicated(subset=['State', 'SourceName', 'Agency', 'TableType','Year']).any()

    def test_check_columns(self, csvfile, source, last):
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

        datasets = get_datasets(csvfile)

        for key in columns.keys():
            assert key in datasets

    def test_table_for_nulls(self, csvfile, source, last):
        can_have_nulls = ["Description", "date_field", "dataset_id", "agency_field", "Year"]
        datasets = get_datasets(csvfile)
        for col in datasets.columns:
            if not col in can_have_nulls:
                assert pd.isnull(datasets[col]).sum() == 0

    
    def test_check_state_names(self, csvfile, source, last):
        all_states = [
            'Alabama', 'Alaska', 'American Samoa', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District Of Columbia',
            'Florida', 'Georgia', 'Guam', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
            'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
            'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio', 'Oklahoma', 'Oregon',
            'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virgin Islands',
            'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
        ]

        datasets = get_datasets(csvfile)
        assert len([x for x in datasets["State"] if x not in all_states]) == 0

    def test_agency_names(self, csvfile, source, last):
        # Agency names should either match source name or be MULTI
        datasets = get_datasets(csvfile)
        rem = datasets["Agency"][datasets["Agency"] != datasets["SourceName"]]
        assert ((rem == opd._datasets.MULTI) | (rem == opd._datasets.NA)).all()

    def test_year(self, csvfile, source, last):
        # year should either be an int or MULTI or "None"
        datasets = get_datasets(csvfile)
        rem = datasets["Year"][[type(x)!=int for x in datasets["Year"]]]
        assert ((rem == opd._datasets.MULTI) | (rem == opd._datasets.NA)).all()

    def test_socrata_id(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        rem = datasets["dataset_id"][datasets["DataType"] == opd._datasets.DataTypes.SOCRATA.value]
        assert pd.isnull(rem).sum() == 0

    def test_years_multi(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        rem = datasets["date_field"][datasets["Year"] == opd._datasets.MULTI]
        assert pd.isnull(rem).sum() == 0

    def test_agencies_multi(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        rem = datasets["agency_field"][datasets["Agency"] == opd._datasets.MULTI]
        assert pd.isnull(rem).sum() == 0

    def test_arcgis_urls(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        urls = datasets["URL"]
        p = re.compile("(MapServer|FeatureServer)/\d+")
        for i,url in enumerate(urls):
            if datasets.iloc[i]["DataType"] == opd._datasets.DataTypes.ArcGIS.value:
                result = p.search(url)
                assert result != None
                assert len(url) == result.span()[1]

    def test_source_list_by_state(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        state = "Virginia"
        df = opd.datasets_query(state=state)
        df_truth = datasets[datasets["State"]==state]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_source_name(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        source_name = "Fairfax County"
        df = opd.datasets_query(source_name=source_name)
        df_truth = datasets[datasets["SourceName"]==source_name]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_agency(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        agency = "Fairfax County"
        df = opd.datasets_query(agency=agency)
        df_truth = datasets[datasets["Agency"]==agency]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_table_type(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        table_type = opd.TableTypes.TRAFFIC
        df = opd.datasets_query(table_type=table_type)
        df_truth = datasets[datasets["TableType"]==table_type.value]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_table_type_value(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        table_type = opd.TableTypes.TRAFFIC.value
        df = opd.datasets_query(table_type=table_type)
        df_truth = datasets[datasets["TableType"]==table_type]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_multi(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        state = "Virginia"
        source_name = "Fairfax County"
        table_type = opd.TableTypes.TRAFFIC_CITATIONS.value
        df = opd.datasets_query(state=state, table_type=table_type, source_name=source_name)
        df_truth = datasets[datasets["TableType"]==table_type]
        df_truth = df_truth[df_truth["State"]==state]
        df_truth = df_truth[df_truth["SourceName"]==source_name]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_table_types(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        for t in datasets["TableType"]:
            # Try to convert to an enum
            opd.TableTypes(t)

    def test_data_types(self, csvfile, source, last):
        datasets = get_datasets(csvfile)
        for t in datasets["DataType"]:
            # Try to convert to an enum
            opd.DataTypes(t)
        

if __name__ == "__main__":
    TestDatasets().test_data_types("C:\\Users\\matth\\repos\\sowd-opd-data\\opd_source_table.csv",None,float('inf'))