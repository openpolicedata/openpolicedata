import pytest
from openpolicedata import datasets

class TestProduct:
    def test_source_list_get_all(self):
        df = datasets.get()
        assert (df == datasets.datasets).all().all()

    def test_source_list_by_state(self):
        state = "Virginia"
        df = datasets.get(state=state)
        df_truth = datasets.datasets[datasets.datasets["State"]==state]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_source_name(self):
        source_name = "Fairfax County Police Department"
        df = datasets.get(source_name=source_name)
        df_truth = datasets.datasets[datasets.datasets["SourceName"]==source_name]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_jurisdiction(self):
        jurisdiction = "Fairfax County Police Department"
        df = datasets.get(jurisdiction=jurisdiction)
        df_truth = datasets.datasets[datasets.datasets["Jurisdiction"]==jurisdiction]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_table_type(self):
        table_type = datasets.TableTypes.ARRESTS
        df = datasets.get(table_type=table_type)
        df_truth = datasets.datasets[datasets.datasets["TableType"]==table_type.value]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_table_type_value(self):
        table_type = datasets.TableTypes.ARRESTS.value
        df = datasets.get(table_type=table_type)
        df_truth = datasets.datasets[datasets.datasets["TableType"]==table_type]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_multi(self):
        state = "Virginia"
        source_name = "Fairfax County Police Department"
        table_type = datasets.TableTypes.ARRESTS.value
        df = datasets.get(state=state, table_type=table_type, source_name=source_name)
        df_truth = datasets.datasets[datasets.datasets["TableType"]==table_type]
        df_truth = df_truth[df_truth["State"]==state]
        df_truth = df_truth[df_truth["SourceName"]==source_name]
        assert len(df)>0
        assert (df == df_truth).all().all()
        