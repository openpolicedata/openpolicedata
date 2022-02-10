import pytest
if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import openpolicedata as opd

class TestProduct:
    def test_data_field_req(self):
        builder = opd._datasets._DatasetBuilder()
        with pytest.raises(ValueError) as e_info:
            builder.add_data("New York", "", opd.TableTypes.STOPS, "", opd.DataTypes.ArcGIS,
                            years=opd._datasets.MULTI)

    def test_jurisdiction_field_req(self):
        builder = opd._datasets._DatasetBuilder()
        with pytest.raises(ValueError) as e_info:
            builder.add_data("New York", opd._datasets.MULTI, opd.TableTypes.STOPS, "", opd.DataTypes.ArcGIS, years=2020)
            
    def test_socrata_id_field_req(self):
        builder = opd._datasets._DatasetBuilder()
        with pytest.raises(ValueError) as e_info:
            builder.add_data("New York", "", opd.TableTypes.STOPS, "", opd.DataTypes.SOCRATA, years=2020)

    def test_source_list_get_all(self):
        df = opd.get()
        assert (df == opd.datasets).all().all()

    def test_source_list_by_state(self):
        state = "Virginia"
        df = opd.get(state=state)
        df_truth = opd.datasets[opd.datasets["State"]==state]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_source_name(self):
        source_name = "Fairfax County Police Department"
        df = opd.get(source_name=source_name)
        df_truth = opd.datasets[opd.datasets["SourceName"]==source_name]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_jurisdiction(self):
        jurisdiction = "Fairfax County Police Department"
        df = opd.get(jurisdiction=jurisdiction)
        df_truth = opd.datasets[opd.datasets["Jurisdiction"]==jurisdiction]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_table_type(self):
        table_type = opd.TableTypes.ARRESTS
        df = opd.get(table_type=table_type)
        df_truth = opd.datasets[opd.datasets["TableType"]==table_type.value]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_table_type_value(self):
        table_type = opd.TableTypes.ARRESTS.value
        df = opd.get(table_type=table_type)
        df_truth = opd.datasets[opd.datasets["TableType"]==table_type]
        assert len(df)>0
        assert (df == df_truth).all().all()

    def test_source_list_by_multi(self):
        state = "Virginia"
        source_name = "Fairfax County Police Department"
        table_type = opd.TableTypes.ARRESTS.value
        df = opd.get(state=state, table_type=table_type, source_name=source_name)
        df_truth = opd.datasets[opd.datasets["TableType"]==table_type]
        df_truth = df_truth[df_truth["State"]==state]
        df_truth = df_truth[df_truth["SourceName"]==source_name]
        assert len(df)>0
        assert (df == df_truth).all().all()
        

if __name__ == "__main__":
    TestProduct().test_data_field_req()