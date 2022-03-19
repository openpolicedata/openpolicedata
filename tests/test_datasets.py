import pytest
import pandas as pd
import numpy as np
import re

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import openpolicedata as opd

class TestProduct:
    def test_duplicates(self):
        assert not opd.datasets.duplicated(subset=['State', 'SourceName', 'Jurisdiction', 'TableType','Year']).any()

    def test_check_columns(self):
        columns = {
            'State' : pd.StringDtype(),
            'SourceName' : pd.StringDtype(),
            'Jurisdiction': pd.StringDtype(),
            'TableType': pd.StringDtype(),
            'Year': np.dtype("O"),
            'Description': pd.StringDtype(),
            'DataType': pd.StringDtype(),
            'URL': pd.StringDtype(),
            'date_field': pd.StringDtype(),
            'dataset_id': pd.StringDtype(),
            'jurisdiction_field': pd.StringDtype()
        }

        for key in columns.keys():
            assert key in opd.datasets

    def test_table_for_nulls(self):
        can_have_nulls = ["Description", "date_field", "dataset_id", "jurisdiction_field", "Year"]

        for col in opd.datasets.columns:
            if not col in can_have_nulls:
                assert pd.isnull(opd.datasets[col]).sum() == 0

    
    def test_check_state_names(self):
        all_states = [
            'Alabama', 'Alaska', 'American Samoa', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District Of Columbia',
            'Florida', 'Georgia', 'Guam', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
            'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
            'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio', 'Oklahoma', 'Oregon',
            'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virgin Islands',
            'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
        ]

        assert len([x for x in opd.datasets["State"] if x not in all_states]) == 0

    def test_jurisdiction_names(self):
        # Jurisdiction names should either match source name or be MULTI
        rem = opd.datasets["Jurisdiction"][opd.datasets["Jurisdiction"] != opd.datasets["SourceName"]]
        assert (rem == opd._datasets.MULTI).all()

    def test_year(self):
        # year should either be an int or MULTI or "None"
        rem = opd.datasets["Year"][[type(x)!=int for x in opd.datasets["Year"]]]
        rem = rem[rem != opd._datasets.NA]
        assert (rem == opd._datasets.MULTI).all()

    def test_socrata_id(self):
        rem = opd.datasets["dataset_id"][opd.datasets["DataType"] == opd._datasets.DataTypes.SOCRATA.value]
        assert pd.isnull(rem).sum() == 0

    def test_years_multi(self):
        rem = opd.datasets["date_field"][opd.datasets["Year"] == opd._datasets.MULTI]
        assert pd.isnull(rem).sum() == 0

    def test_jurisdictions_multi(self):
        rem = opd.datasets["jurisdiction_field"][opd.datasets["Jurisdiction"] == opd._datasets.MULTI]
        assert pd.isnull(rem).sum() == 0

    def test_arcgis_urls(self):
        urls = opd.datasets["URL"]
        p = re.compile("(MapServer|FeatureServer)/\d+")
        for i,url in enumerate(urls):
            if opd.datasets.iloc[i]["DataType"] == opd._datasets.DataTypes.ArcGIS.value:
                result = p.search(url)
                assert result != None
                assert len(url) == result.span()[1]

    def test_source_list_get_all(self):
        df = opd.datasets_query()
        assert df.equals(opd.datasets)

    def test_source_list_by_state(self):
        state = "Virginia"
        df = opd.datasets_query(state=state)
        df_truth = opd.datasets[opd.datasets["State"]==state]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_source_name(self):
        source_name = "Fairfax County"
        df = opd.datasets_query(source_name=source_name)
        df_truth = opd.datasets[opd.datasets["SourceName"]==source_name]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_jurisdiction(self):
        jurisdiction = "Fairfax County"
        df = opd.datasets_query(jurisdiction=jurisdiction)
        df_truth = opd.datasets[opd.datasets["Jurisdiction"]==jurisdiction]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_table_type(self):
        table_type = opd.TableTypes.ARRESTS
        df = opd.datasets_query(table_type=table_type)
        df_truth = opd.datasets[opd.datasets["TableType"]==table_type.value]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_table_type_value(self):
        table_type = opd.TableTypes.ARRESTS.value
        df = opd.datasets_query(table_type=table_type)
        df_truth = opd.datasets[opd.datasets["TableType"]==table_type]
        assert len(df)>0
        assert df_truth.equals(df)

    def test_source_list_by_multi(self):
        state = "Virginia"
        source_name = "Fairfax County"
        table_type = opd.TableTypes.ARRESTS.value
        df = opd.datasets_query(state=state, table_type=table_type, source_name=source_name)
        df_truth = opd.datasets[opd.datasets["TableType"]==table_type]
        df_truth = df_truth[df_truth["State"]==state]
        df_truth = df_truth[df_truth["SourceName"]==source_name]
        assert len(df)>0
        assert df_truth.equals(df)
        

if __name__ == "__main__":
    TestProduct().test_source_list_get_all()