import pytest
if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders
import pandas as pd
try:
    import geopandas as gpd
    _has_gpd = True
except:
    _has_gpd = False

import warnings
warnings.filterwarnings(action='ignore', module='arcgis')

class TestProduct:
    def test_process_date_input_empty(self, csvfile, source, last, skip, loghtml):
        with pytest.raises(ValueError):
            data_loaders._process_date([])
        
    def test_process_date_too_many(self, csvfile, source, last, skip, loghtml):
        year = [2021, 2022, 2023]
        with pytest.raises(ValueError):
            data_loaders._process_date(year)

    def test_process_dates_year_input_wrong_order(self, csvfile, source, last, skip, loghtml):
        year = [2023, 2021]
        with pytest.raises(ValueError):
            data_loaders._process_date(year)

    def test_arcgis(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16/"
        gis = data_loaders.Arcgis(url)
        df = gis.load()
        count = gis.get_count()
        
        data_loaders._default_limit = lim

        if url[-1] == "/":
            url = url[0:-1]
        last_slash = url.rindex("/")
        layer_num = url[last_slash+1:]
        base_url = url[:last_slash]
        layer_collection = data_loaders.FeatureLayerCollection(base_url)

        is_table = True
        active_layer = None
        for layer in layer_collection.layers:
            layer_url = layer.url
            if layer_url[-1] == "/":
                layer_url = layer_url[:-1]
            if layer_num == layer_url[last_slash+1:]:
                active_layer = layer
                is_table = False
                break

        if is_table:
            for layer in layer_collection.tables:
                layer_url = layer.url
                if layer_url[-1] == "/":
                    layer_url = layer_url[:-1]
                if layer_num == layer_url[last_slash+1:]:
                    active_layer = layer
                    break

        layer_query_result = active_layer.query(as_df=True)

        assert set(df.columns) == set(layer_query_result.columns)
        assert len(layer_query_result) == count
        df = df[layer_query_result.columns]

        assert layer_query_result.equals(df)

    def test_arcgis_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
            date_field = "TIME_PHONEPICKUP"
            year_filter = 2020
            limit = 1000
            df = data_loaders.Arcgis(url, date_field=date_field).load(year=year_filter, limit=limit)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_arcgis_pandas(self, csvfile, source, last, skip, loghtml):
        data_loaders._use_gpd_force = False
        url = "https://services1.arcgis.com/wpJGOi6N4Rq5cqFv/arcgis/rest/services/Pursuits_2020_2021/FeatureServer/0/"
        date_field = "DATE"
        year_filter = 2020
        gis = data_loaders.Arcgis(url, date_field=date_field)
        df = gis.load(year=year_filter)
        count = gis.get_count(year=year_filter)
        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame
        assert len(df) == count

    def test_socrata_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "data.montgomerycountymd.gov"
            data_set = "4mse-ku6q"
            date_field = "date_of_stop"
            year = 2020
            limit = 1000
            df = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field).load(year=year, limit=limit)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_socrata_pandas(self, csvfile, source, last, skip, loghtml):
        data_loaders._use_gpd_force = False
        url = "data.montgomerycountymd.gov"
        data_set = "usip-62e2"
        date_field = "created_dt"
        year = 2020
        loader = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field)
        df = loader.load(year=year)
        count = loader.get_count(year=year)

        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame
        assert len(df) == count

    def test_socrata(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "data.austintexas.gov"
        data_set = "sc8s-w4ka"
        loader = data_loaders.Socrata(url, data_set)
        df =loader.load()
        count = loader.get_count()
        
        data_loaders._default_limit = lim

        client = data_loaders.SocrataClient(url, data_loaders.default_sodapy_key, timeout=60)
        results = client.get(data_set, order=":id", limit=100000)
        rows = pd.DataFrame.from_records(results)

        assert len(df) == count
        assert rows.equals(df)

    def test_csv(self, csvfile, source, last, skip, loghtml):
        url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
        loader = data_loaders.Csv(url)
        df = loader.load()
        df_comp = pd.read_csv(url)

        count = loader.get_count()
        assert len(df_comp) == count

        assert df_comp.equals(df)

        with pytest.raises(ValueError):
            years = loader.get_years(force=True)

        date_field = "INCIDENT_DATE"
        loader = data_loaders.Csv(url, date_field=date_field)
        with pytest.raises(ValueError):
            loader.get_years()

        years = loader.get_years(force=True)

        df = df.astype({date_field: 'datetime64[ns]'})
        assert list(df[date_field].dt.year.sort_values(ascending=False).unique()) == years

        nrows = 7
        df = data_loaders.Csv(url).load(limit=nrows)
        df_comp = pd.read_csv(url, nrows=nrows)

        assert df_comp.equals(df)

    def test_excel(self, csvfile, source, last, skip, loghtml):
        url = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/DeathInCustody_2005-2021.xlsx"
        df = data_loaders.load_excel(url)
        df_comp = pd.read_excel(url)
        assert df_comp.equals(df)

        nrows = 7
        df = data_loaders.load_excel(url, limit=nrows)        
        df_comp = pd.read_excel(url, nrows=nrows)
        assert df_comp.equals(df)


    def test_csv_year_filter(self, csvfile, source, last, skip, loghtml):
        url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
        loader = data_loaders.Csv(url, date_field="INCIDENT_DATE")
        year = 2020
        df = loader.load(year=year)
        with pytest.raises(ValueError):
            count = loader.get_count(year=year)

        count = loader.get_count(year=year, force=True)
        assert len(df) == count


if __name__ == "__main__":
    tp = TestProduct()
    tp.test_arcgis(None,None,None,None,None)
    tp.test_arcgis_geopandas(None,None,None,None,None)
    tp.test_arcgis_pandas(None,None,None,None,None)
    tp.test_csv(None,None,None,None,None)
    tp.test_csv_year_filter(None,None,None,None,None)
    tp.test_process_date_input_empty(None,None,None,None,None)
    tp.test_process_date_too_many(None,None,None,None,None)
    tp.test_process_dates_year_input_wrong_order(None,None,None,None,None)
    tp.test_socrata(None,None,None,None,None)
    tp.test_socrata_geopandas(None,None,None,None,None)
    tp.test_socrata_pandas(None,None,None,None,None)