import pytest
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


    @pytest.mark.skip(reason="This is a slow test and should only be run before a major commit.")
    def test_url_arcgis_to_dataframe(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        dateField = 'TIME_PHONEPICKUP'
        year = 2021
        df=DataLoaders.loadArcGIS(url, dateField=dateField, year=year)
        print(df)
        assert len(df)>0
        