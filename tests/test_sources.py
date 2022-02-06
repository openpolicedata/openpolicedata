import pytest
from openpolicedata import data
from openpolicedata.datasets import datasets

class TestProduct:
	def test_source_urls_limitable(self):
		for i in range(len(datasets)):
			if self.can_be_limited(datasets.iloc[i]["DataType"]):
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)
				# For speed, set private limit parameter so that only a single entry is requested
				src._Source__limit = 1

				table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
				assert len(table.table)==1

				# TODO: test date fields
				
				
	def test_jurisdiction_filter(self):
		src = data.Source("Virginia Community Policing Act")
		jurisdiction="Fairfax County Police Department"
		# For speed, set private limit parameter so that only a single entry is requested
		src._Source__limit = 100
		table = src.load_from_url(2021, jurisdiction_filter=jurisdiction)
		
		assert len(table.table)==100
		assert table.table[table._jurisdiction_field].nunique()==1
		assert table.table.iloc[0][table._jurisdiction_field] == jurisdiction
			
			
	@pytest.mark.slow(reason="This is a slow test and should only be run before a major commit.")
	def test_source_urls_unlimitable(self):
		for i in range(len(datasets)):
			if not self.can_be_limited(datasets.iloc[i]["DataType"]):
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
				assert len(table.table)>0

	# TODO: Future tests on date filtering, get year and jurisdictions functions, and a couple of testings that read in the whole table...
	# TODO: test to ensure that loaded data is geopandas and has geometry where appropriate
	def can_be_limited(self, table_type):
		if (table_type == "CSV" or table_type == "GeoJSON"):
			return False
		elif (table_type == "ArcGIS" or table_type == "Socrata"):
			return True
		else:
		  	raise ValueError("Unknown table type")		
