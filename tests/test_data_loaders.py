import pytest
if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders


class TestProduct:
    def test_arcgis_year_input_empty(self, csvfile):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
        date_field = 'TIME_PHONEPICKUP'
        year = []
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year)
        
    def test_arcgis_year_input_too_many(self, csvfile):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
        date_field = 'TIME_PHONEPICKUP'
        year = [2021, 2022, 2023]
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year) 

    def test_arcgis_year_input_wrong_order(self, csvfile):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer/32'
        date_field = 'TIME_PHONEPICKUP'
        year = [2023, 2021]
        with pytest.raises(ValueError) as e_info:
                df=data_loaders.load_arcgis(url, date_field=date_field, year=year) 


if __name__ == "__main__":
    tp = TestProduct()
    tp.test_arcgis_year_input_empty()