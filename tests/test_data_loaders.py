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
        df = data_loaders.load_arcgis(url)
        
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
        df = df[layer_query_result.columns]

        assert layer_query_result.equals(df)

    def test_arcgis_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
            date_field = "TIME_PHONEPICKUP"
            year_filter = 2020
            limit = 1000
            df = data_loaders.load_arcgis(url, date_field=date_field, year=year_filter, limit=limit)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_arcgis_pandas(self, csvfile, source, last, skip, loghtml):
        data_loaders._use_gpd_force = False
        url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
        date_field = "TIME_PHONEPICKUP"
        year_filter = 2020
        limit = 1000
        df = data_loaders.load_arcgis(url, date_field=date_field, year=year_filter, limit=limit)
        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame

    def test_socrata_geopandas(self, csvfile, source, last, skip, loghtml):
        if _has_gpd:
            url = "data.montgomerycountymd.gov"
            data_set = "4mse-ku6q"
            date_field = "date_of_stop"
            year = 2020
            limit = 1000
            df = data_loaders.load_socrata(url, data_set, date_field=date_field, year=year, 
                limit=limit)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_socrata_pandas(self, csvfile, source, last, skip, loghtml):
        data_loaders._use_gpd_force = False
        url = "data.montgomerycountymd.gov"
        data_set = "4mse-ku6q"
        date_field = "date_of_stop"
        year = 2020
        limit = 1000
        df = data_loaders.load_socrata(url, data_set, date_field=date_field, year=year, 
            limit=limit)
        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame

    def test_socrata(self, csvfile, source, last, skip, loghtml):
        lim = data_loaders._default_limit
        data_loaders._default_limit = 500
        url = "data.austintexas.gov"
        data_set = "sc8s-w4ka"
        df = data_loaders.load_socrata(url, data_set)
        
        data_loaders._default_limit = lim

        client = data_loaders.Socrata(url, data_loaders.default_sodapy_key, timeout=60)
        results = client.get(data_set, order=":id", limit=100000)
        rows = pd.DataFrame.from_records(results)

        assert rows.equals(df)

    def test_csv(self, csvfile, source, last, skip, loghtml):
        url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
        df = data_loaders.load_csv(url)
        df_comp = pd.read_csv(url)
        assert df_comp.equals(df)

        nrows = 7
        df = data_loaders.load_csv(url, limit=nrows)
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


if __name__ == "__main__":
    tp = TestProduct()
    tp.test_arcgis(None,None,None,None,None)