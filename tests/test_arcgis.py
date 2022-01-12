import pytest
import pandas as pd
import os
import DataLoaders

class TestProduct:
    def test_arcgis_year_input_empty(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        dateField = 'TIME_PHONEPICKUP'
        year = []
        with pytest.raises(ValueError) as e_info:
                df=DataLoaders.loadArcGIS(url, dateField=dateField, year=year)
        
    def test_arcgis_year_input_too_many(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        dateField = 'TIME_PHONEPICKUP'
        year = [2021, 2022, 2023]
        with pytest.raises(ValueError) as e_info:
                df=DataLoaders.loadArcGIS(url, dateField=dateField, year=year) 

    def test_arcgis_year_input_wrong_order(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        dateField = 'TIME_PHONEPICKUP'
        year = [2023, 2021]
        with pytest.raises(ValueError) as e_info:
                df=DataLoaders.loadArcGIS(url, dateField=dateField, year=year) 


    @pytest.mark.slow(reason="This is a slow test and should only be run before a major commit.")
    def test_url_arcgis_to_dataframe(self):        
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        dateField = 'TIME_PHONEPICKUP'
        gold_start_year = 2014
        gold_end_year = 2016
        gold_filename = f'./sandbox/data/police_pedestrian_stops_and_vehicle_stops.gold.{gold_start_year}_{gold_end_year}.csv'         
        print(f'Comparing {url} with {gold_filename}')
        print(f'Current directory is {os.getcwd()}')
        df_gold = pd.read_csv(gold_filename)
        df=DataLoaders.loadArcGIS(url, dateField=dateField, year=[gold_start_year, gold_end_year])
        assert len(df) == len(df_gold)
        
