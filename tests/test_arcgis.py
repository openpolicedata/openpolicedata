import pytest
import pandas as pd
import os
import sys
from openpolicedata import data_loaders
from openpolicedata import data
from openpolicedata import datasets


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
        gold_start_year = 2014
        gold_end_year = 2016
        gold_filename = f'./data/police_pedestrian_stops_and_vehicle_stops.gold.{gold_start_year}_{gold_end_year}.csv'
        if os.path.isfile(gold_filename):
            print(f'Comparing {url} with {gold_filename}')
            print(f'Current directory is {os.getcwd()}')
            df_gold = pd.read_csv(gold_filename)
        else;
            csv_url = "https://www.denvergov.org/media/gis/DataCatalog/police_pedestrian_stops_and_vehicle_stops/csv/police_pedestrian_stops_and_vehicle_stops.csv"
            df_gold = pd.read_csv(csv_url)
            if not os.path.isdir(os.path.dirname(gold_filename)):
                os.mkdir(os.path.dirname(gold_filename))
            df_gold.to_csv(gold_filename)
            
        src = data.Source("Denver Police Department")
        table = src.load_from_url([gold_start_year, gold_end_year], table_type=datasets.TableTypes.STOPS)
        assert len(table.table) == len(df_gold)
        
