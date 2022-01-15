import pytest
import pandas as pd
import os
import sys

try:
    import data_loaders
except ModuleNotFoundError as error:
    sys.exit(str(error) + ". Possible issue: The src directory is not on your path. If running Linux did you first run export PYTHONPATH=./src:${PYTHONPATH} ?")
except Exception as exception:
    # Output unexpected Exceptions.
    sys.exit(str(exception))



class TestProduct:
    def test_arcgis_year_input_empty(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        date_field = 'TIME_PHONEPICKUP'
        year = []
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year)
        
    def test_arcgis_year_input_too_many(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        date_field = 'TIME_PHONEPICKUP'
        year = [2021, 2022, 2023]
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year) 

    def test_arcgis_year_input_wrong_order(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        date_field = 'TIME_PHONEPICKUP'
        year = [2023, 2021]
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year) 


    @pytest.mark.slow(reason="This is a slow test and should only be run before a major commit.")
    def test_url_arcgis_to_dataframe(self):        
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        date_field = 'TIME_PHONEPICKUP'
        gold_start_year = 2014
        gold_end_year = 2016
        gold_filename = f'./data/police_pedestrian_stops_and_vehicle_stops.gold.{gold_start_year}_{gold_end_year}.csv'         
        print(f'Comparing {url} with {gold_filename}')
        print(f'Current directory is {os.getcwd()}')
        df_gold = pd.read_csv(gold_filename)
        df=data_loaders.loadArcGIS(url, date_field=date_field, year=[gold_start_year, gold_end_year])
        assert len(df) == len(df_gold)
        
