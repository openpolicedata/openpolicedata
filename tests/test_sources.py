import pytest
import requests
if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import datasets
from openpolicedata import _datasets
import random
from datetime import datetime

class TestProduct:
	def test_source_url_name_unlimitable(self):
		for i in range(len(datasets)):
			if not self.can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
				ext = "." + datasets.iloc[i]["DataType"].lower()
				assert ext in datasets.iloc[i]["URL"]


	def test_source_urls(self):
		for i in range(len(datasets)):
			url = datasets.iloc[i]["URL"]
			try:
				r = requests.head(url)
			except requests.exceptions.MissingSchema:
				if url[0:4] != "http":
					https = "https://"
					url = https + url
					r = requests.head(url)
				else:
					raise
			except:
				raise

			if r.status_code != 200:
				raise ValueError(f"Status code for {url} is {r.status_code}")


	def test_source_download_limitable(self):
		for i in range(len(datasets)):
			if self.can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)
				# For speed, set private limit parameter so that only a single entry is requested
				src._Source__limit = 1

				table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
				assert len(table.table)==1
				if "date_field" in datasets.iloc[i]["LUT"]:
					assert datasets.iloc[i]["LUT"]["date_field"] in table.table
					assert table.table[datasets.iloc[i]["LUT"]["date_field"]].dtype.name == 'datetime64[ns]'
				if "jurisdiction_field" in datasets.iloc[i]["LUT"]:
					assert datasets.iloc[i]["LUT"]["jurisdiction_field"] in table.table

					
	def test_get_years(self):
		for i in range(len(datasets)):
			if self.is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Year"] != _datasets.MULTI:
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				years = src.get_years(datasets.iloc[i]["TableType"])

				if datasets.iloc[i]["Year"] != _datasets.MULTI:
					assert datasets.iloc[i]["Year"] in years
				else:
					assert len(years) > 0

	
	def test_get_jurisdictions(self):
		for i in range(len(datasets)):
			if self.is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Jurisdiction"] != _datasets.MULTI:
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				jurisdictions = src.get_jurisdictions(datasets.iloc[i]["TableType"], year=datasets.iloc[i]["Year"])

				if datasets.iloc[i]["Jurisdiction"] != _datasets.MULTI:
					assert [datasets.iloc[i]["Jurisdiction"]] == jurisdictions
				else:
					assert len(jurisdictions) > 0


	def test_get_jurisdictions_name_match(self):
		src = data.Source("Virginia Community Policing Act")

		jurisdictions = src.get_jurisdictions(partial_name="Arlington")

		assert len(jurisdictions) == 2
				
				
	def test_jurisdiction_filter(self):
		src = data.Source("Virginia Community Policing Act")
		jurisdiction="Fairfax County Police Department"
		# For speed, set private limit parameter so that only a single entry is requested
		src._Source__limit = 100
		table = src.load_from_url(2021, jurisdiction_filter=jurisdiction)
		
		assert len(table.table)==100
		assert table.table[table._jurisdiction_field].nunique()==1
		assert table.table.iloc[0][table._jurisdiction_field] == jurisdiction

	
	@pytest.mark.slow(reason="This is a slow test tgat should be run before a major commit.")
	def test_load_year(self):
		# Test that filtering for a year works at the boundaries
		for i in range(len(datasets)):
			if self.is_filterable(datasets.iloc[i]["DataType"]) and datasets.iloc[i]["Year"] == _datasets.MULTI:
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]

				if datasets.iloc[i]["Jurisdiction"] == _datasets.MULTI and \
					srcName == "Virginia Community Policing Act":
					# Reduce size of data load by filtering by jurisdiction
					jurisdiction_filter = "Henrico Police Department"
				else:
					jurisdiction_filter = None

				src = data.Source(srcName, state=state)

				years = src.get_years(datasets.iloc[i]["TableType"])

				if len(years)>1:
					# It is preferred to to not use first or last year that start and stop of year are correct
					year = years[1]
				else:
					year = years[0]

				# table_print = datasets.iloc[i]["TableType"]
				# now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				# print(f"{now }Testing {i} of {len(datasets)}: {srcName} {table_print} table for {year}")

				table = src.load_from_url(year, datasets.iloc[i]["TableType"], 
										jurisdiction_filter=jurisdiction_filter)

				dts = table.table[datasets.iloc[i]["LUT"]["date_field"]]
				dts = dts.sort_values(ignore_index=True)

				all_years = dts.dt.year.unique().tolist()

				assert len(all_years) == 1
				assert all_years[0] == year

				start_date = str(year-1) + "-12-29"
				stop_date  = str(year) + "-01-10"  

				table_start = src.load_from_url([start_date, stop_date], datasets.iloc[i]["TableType"], 
												jurisdiction_filter=jurisdiction_filter)
				dts_start = table_start.table[datasets.iloc[i]["LUT"]["date_field"]]

				dts_start = dts_start.sort_values(ignore_index=True)

				# If this isn't true then the stop date is too early
				assert dts_start.iloc[-1].year == year

				# Find first value date in year
				dts_start = dts_start[dts_start.dt.year == year]
				assert dts.iloc[0] == dts_start.iloc[0]

				start_date = str(year) + "-12-20"
				stop_date  = str(year+1) + "-01-10"  

				table_stop = src.load_from_url([start_date, stop_date], datasets.iloc[i]["TableType"], 
												jurisdiction_filter=jurisdiction_filter)
				dts_stop = table_stop.table[datasets.iloc[i]["LUT"]["date_field"]]

				dts_stop = dts_stop.sort_values(ignore_index=True)

				# If this isn't true then the start date is too late
				assert dts_stop.iloc[0].year == year

				# Find last value date in year
				dts_stop = dts_stop[dts_stop.dt.year == year]
				assert dts.iloc[-1] == dts_stop.iloc[-1]


	@pytest.mark.slow(reason="This is a slow test and should be run before a major commit.")
	def test_source_download_not_limitable(self):
		for i in range(len(datasets)):
			if not self.can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
				if "stanford.edu" in datasets.iloc[i]["URL"]:
					# There are a lot of data sets from Stanford, no need to run them all
					# Just run approximately 10%
					rnd = random.uniform(0,1)
					if rnd>0.05:
						continue

				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				year = datasets.iloc[i]["Year"]
				table_type = datasets.iloc[i]["TableType"]

				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i} of {len(datasets)}: {srcName}, {state} {table_type} table for {year}")
				try:
					table = src.load_from_url(year, table_type)
				except:
					raise ValueError(f"Error loading CSV {srcName}, year={year}, table_type={table_type}")

				assert len(table.table)>1
				if "date_field" in datasets.iloc[i]["LUT"]:
					assert datasets.iloc[i]["LUT"]["date_field"] in table.table
					assert table.table[datasets.iloc[i]["LUT"]["date_field"]].dtype.name == 'datetime64[ns]'
				if "jurisdiction_field" in datasets.iloc[i]["LUT"]:
					assert datasets.iloc[i]["LUT"]["jurisdiction_field"] in table.table


	def can_be_limited(self, table_type, url):
		if table_type == "GeoJSON" or (table_type == "CSV" and ".zip" in url):
			return False
		elif (table_type == "ArcGIS" or table_type == "Socrata" or table_type == "CSV"):
			return True
		else:
			raise ValueError("Unknown table type")


	def is_filterable(self, table_type):
		if table_type == "GeoJSON" or table_type == "CSV":
			return False
		elif (table_type == "ArcGIS" or table_type == "Socrata" ):
			return True
		else:
			raise ValueError("Unknown table type")

if __name__ == "__main__":
	# For testing
	tp = TestProduct()
	tp.test_source_download_not_limitable()