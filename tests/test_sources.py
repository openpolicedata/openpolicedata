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
				assert len(table.table)==1
				
				
	def test_jurisdiction_filter(self):
		src = data.Source("Virginia Community Policing Act")
		# For speed, set private limit parameter so that only a single entry is requested
		src._Source__limit = 100
		table = load_from_url(self, 2021, jurisdiction_filter="Fairfax County Police Department")
		
		assert len(table.table)==100
		assert table.table[table._jurisdiction_field].nunique()==1
		assert table.table.iloc[0[[table._jurisdiction_field] == "Fairfax County Police Department"
			
			
	@pytest.mark.slow(reason="This is a slow test and should only be run before a major commit.")
	def test_source_urls_unlimitable(self):
		for i in range(len(datasets)):
		    if not self.can_be_limited(datasets.iloc[i]["TableType"]):
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				table = self.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
				assert len(table.table)>0
        
      
	def can_be_limited(self, table_type):
    	if (table_type == "CSV" or table_type == "GeoJSON"):
      		return False
		elif (table_type == "ArcGIS" or table_type == "Socrata"):
		  	return True
		else:
		  	raise ValueError("Unknown table type")
