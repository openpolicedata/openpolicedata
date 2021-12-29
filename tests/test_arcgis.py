import DataLoaders

class TestProduct:
    def test_url_arcgis_to_dataframe(self):
        url = 'https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_STOPS_P/FeatureServer'
        dateField = 'TIME_PHONEPICKUP'
        year = 2021
        df=DataLoaders.arcGIS(url, dateField=dateField, year=year)
        print(df)
        assert len(df)>0
        