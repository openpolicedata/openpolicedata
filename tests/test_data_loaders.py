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
    def test_arcgis_year_input_empty(self, csvfile, source, last, skip):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
        date_field = 'TIME_PHONEPICKUP'
        year = []
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year)
        
    def test_arcgis_year_input_too_many(self, csvfile, source, last, skip):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
        date_field = 'TIME_PHONEPICKUP'
        year = [2021, 2022, 2023]
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year) 

    def test_arcgis_year_input_wrong_order(self, csvfile, source, last, skip):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
        date_field = 'TIME_PHONEPICKUP'
        year = [2023, 2021]
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year) 

    def test_arcgis_geopandas(self, csvfile, source, last, skip):
        if _has_gpd:
            url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
            date_field = "TIME_PHONEPICKUP"
            year_filter = 2020
            limit = 1000
            df = data_loaders.load_arcgis(url, date_field=date_field, year=year_filter, limit=limit)

            assert type(df) == gpd.GeoDataFrame
        else:
            pass

    def test_arcgis_pandas(self, csvfile, source, last, skip):
        data_loaders._use_gpd_force = False
        url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32/"
        date_field = "TIME_PHONEPICKUP"
        year_filter = 2020
        limit = 1000
        df = data_loaders.load_arcgis(url, date_field=date_field, year=year_filter, limit=limit)
        # Reset
        data_loaders._use_gpd_force = None

        assert type(df) == pd.DataFrame

    def test_socrata_geopandas(self, csvfile, source, last, skip):
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

    def test_socrata_pandas(self, csvfile, source, last, skip):
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


if __name__ == "__main__":
    tp = TestProduct()
    tp.test_arcgis_pandas(None,None,None)