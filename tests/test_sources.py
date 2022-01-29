import pytest
from openpolicedata import data
from openpolicedata.datasets import datasets

class TestProduct:
	def test_source_urls_limitable(self):
		for i in range(len(datasets)):
      if self.can_be_limited(datasets.iloc[i]["TableType"]):
        srcName = datasets.iloc[i]["SourceName"]
        state = datasets.iloc[i]["State"]
        src = data.Source(srcName, state=state)
        # For speed, set private limit parameter so that only a single entry is requested
        src._Source__limit = 1
        
        table = self.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
        assert len(table.table)>0
        
      
  def can_be_limited(self, table_type):
    if (table_type == "CSV" or table_type == "GeoJSON"):
      return False
    elif (table_type == "ArcGIS" or table_type == "Socrata"):
      return True
    else:
      raise ValueError("Unknown table type")
