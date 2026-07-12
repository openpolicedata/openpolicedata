import pytest
import requests
import sys

if __name__ == "__main__":
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

def test_arcgis_two_digit_year_text_date_query(monkeypatch):
    url = "https://example.com/arcgis/rest/services/Test/FeatureServer/0"
    loader = data_loaders.Arcgis.__new__(data_loaders.Arcgis)
    loader.url = url
    loader.date_field = "DATE_REPORTED"
    loader.query = {}
    loader._date_type = None
    loader._date_format = None
    loader._year_digits = 4
    loader._last_count = None

    sample_data = {
        "fields": [{"name": "DATE_REPORTED", "type": "esriFieldTypeString"}],
        "features": [
            {"attributes": {"DATE_REPORTED": "1/1/18 1:00 AM"}},
            {"attributes": {"DATE_REPORTED": "12/31/18 11:59 PM"}},
        ],
    }
    queries = []

    def request_stub(where=None, return_count=False, **kwargs):
        if where == "DATE_REPORTED IS NOT NULL":
            return sample_data
        queries.append(where)
        return {"count": 2}

    monkeypatch.setattr(loader, "_Arcgis__request", request_stub)

    where_query, record_count = loader._build_date_query([pd.Timestamp("2018-01-01"), pd.Timestamp("2018-12-31")], True)

    assert record_count == 2
    assert queries == [where_query]
    assert "DATE_REPORTED LIKE '_/_/18 %'" in where_query
    assert "DATE_REPORTED LIKE '__/__/18'" in where_query
    assert "2018" not in where_query
