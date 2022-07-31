import pytest
import requests

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import _datasets
from openpolicedata import datasets_query
from openpolicedata.defs import MULTI, DataType
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
import random
from datetime import datetime
from datetime import timedelta
import pandas as pd
from time import sleep
import warnings
import os

sleep_time = 0.1
log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
log_folder = os.path.join(".","data/test_logs")

warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError)

def get_datasets(csvfile):
    if csvfile != None:
        _datasets.datasets = _datasets._build(csvfile)

    return datasets_query()

class TestData:
	def test_source_url_name_unlimitable(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		for i in range(len(datasets)):			
			if i < len(datasets) - last:
				continue
			
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue
			if not can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
				ext = "." + datasets.iloc[i]["DataType"].lower()
				if ext == ".csv":
					# -csv.zip in NYC data
					assert ext in datasets.iloc[i]["URL"] or "-csv.zip" in datasets.iloc[i]["URL"]
				else:
					assert ext in datasets.iloc[i]["URL"]


	def test_source_urls(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]

		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue

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

			# 200 is success
			# 301 is moved permanently. This is most likely NYC. In this case, the main site has moved but the datasets have not
			if r.status_code != 200 and r.status_code != 301:
				r = requests.get(url)
				if r.status_code != 200 and r.status_code != 301:
					raise ValueError(f"Status code for {url} is {r.status_code}")

			# Adding a pause here to prevent issues with requesting from site too frequently
			sleep(sleep_time)

	def test_check_version(self, csvfile, source, last, skip, loghtml):
		datasets = get_datasets(csvfile)
		ds = datasets[(datasets["Year"]==MULTI)]
		if len(ds)>0:
			ds = ds.iloc[0]
			src = data.Source(ds["SourceName"], state=ds["State"])
			for k, year in enumerate(src.datasets["Year"]):
				if year == MULTI:
					break
			# Set min_version to create error
			src.datasets.loc[k, "min_version"] = "-1"
			with pytest.raises(OPD_FutureError):
				src.get_years(src.datasets.loc[k, "TableType"])

			src.datasets.loc[k, "min_version"] = "0.0"
			with pytest.raises(OPD_MinVersionError):
				src.get_years(src.datasets.loc[k, "TableType"])

			# These should pass
			src.datasets.loc[k, "min_version"] = "100000.0"
			data._check_version(src.datasets.loc[k])
			src.datasets.loc[k, "min_version"] = pd.NA
			data._check_version(src.datasets.loc[k])

		ds = datasets[(datasets["Agency"]==MULTI)]
		if len(ds)>0:
			ds = ds.iloc[0]
			src = data.Source(ds["SourceName"], state=ds["State"])
			for k, year in enumerate(src.datasets["Year"]):
				if year == MULTI:
					break
			# Set min_version to create error
			src.datasets.loc[k, "min_version"] = "-1"
			with pytest.raises(OPD_FutureError):
				src.get_agencies(src.datasets.loc[k, "TableType"], year=src.datasets.loc[k, "Year"])

			src.datasets.loc[k, "min_version"] = "0.0"
			with pytest.raises(OPD_MinVersionError):
				src.get_agencies(src.datasets.loc[k, "TableType"], year=src.datasets.loc[k, "Year"])

		ds = datasets
		if len(ds)>0:
			ds = ds.iloc[0]
			src = data.Source(ds["SourceName"], state=ds["State"])
			for k, year in enumerate(src.datasets["Year"]):
				if year == MULTI:
					break
			# Set min_version to create error
			src.datasets.loc[k, "min_version"] = "-1"
			with pytest.raises(OPD_FutureError):
				src.load_from_url(year=src.datasets.loc[k, "Year"], table_type=src.datasets.loc[k, "TableType"])

			src.datasets.loc[k, "min_version"] = "0.0"
			with pytest.raises(OPD_MinVersionError):
				src.load_from_url(year=src.datasets.loc[k, "Year"], table_type=src.datasets.loc[k, "TableType"])


	def test_get_years(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		caught_exceptions = []
		caught_exceptions_warn = []
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]

		for i in range(len(datasets)):
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Year"] != MULTI:
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i} of {len(datasets)}: {srcName} {table_print} table")

				try:
					years = src.get_years(datasets.iloc[i]["TableType"])
				except warn_errors as e:
					e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"])
					caught_exceptions_warn.append(e)
					continue
				except (OPD_TooManyRequestsError, OPD_arcgisAuthInfoError) as e:
					# Catch exceptions related to URLs not functioning
					e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"])
					caught_exceptions.append(e)
					continue
				except:
					raise

				if datasets.iloc[i]["Year"] != MULTI:
					assert datasets.iloc[i]["Year"] in years
				else:
					assert len(years) > 0

				# Adding a pause here to prevent issues with requesting from site too frequently
				sleep(sleep_time)

		if loghtml:
			log_errors_to_file(caught_exceptions, caught_exceptions_warn)
		else:
			if len(caught_exceptions)==1:
				raise caught_exceptions[0]
			elif len(caught_exceptions)>0:
				msg = f"{len(caught_exceptions)} URL errors encountered:\n"
				for e in caught_exceptions:
					msg += "\t" + e.args[0] + "\n"
				raise OPD_MultipleErrors(msg)

			for e in caught_exceptions_warn:
				warnings.warn(str(e))



	def test_source_download_limitable(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		num_stanford = 0
		max_num_stanford = 1  # This data is standardized. Probably no need to test more than 1
		caught_exceptions = []
		caught_exceptions_warn = []
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]
			
		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue
			has_date_field = not pd.isnull(datasets.iloc[i]["date_field"])
			if can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]) or has_date_field:
				if is_stanford(datasets.iloc[i]["URL"]):
					num_stanford += 1
					if num_stanford > max_num_stanford:
						continue
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)
				# For speed, set private limit parameter so that only a single entry is requested
				src._Source__limit = 20

				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i} of {len(datasets)}: {srcName} {table_print} table")

				try:
					table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
				except warn_errors as e:
					e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"])
					caught_exceptions_warn.append(e)
					continue
				except (OPD_TooManyRequestsError, OPD_arcgisAuthInfoError) as e:
					# Catch exceptions related to URLs not functioning
					e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"])
					caught_exceptions.append(e)
					continue
				except:
					raise

				assert len(table.table)>0
				if not pd.isnull(datasets.iloc[i]["date_field"]):
					assert datasets.iloc[i]["date_field"] in table.table
					#assuming a Pandas string dtype('O').name = object is okay too
					assert (table.table[datasets.iloc[i]["date_field"]].dtype.name in ['datetime64[ns]', 'datetime64[ms]'])
					dts = table.table[datasets.iloc[i]["date_field"]]
					dts = dts[dts.notnull()]
					assert len(dts) > 0   # If not, either all dates are bad or number of rows requested needs increased
					# Check that year is reasonable
					assert dts.iloc[0].year >= 1970  # This is just an arbitrarily old year that is assumed to be before all available data
					assert dts.iloc[0].year <= datetime.now().year
				if not pd.isnull(datasets.iloc[i]["agency_field"]):
					assert datasets.iloc[i]["agency_field"] in table.table

				# Adding a pause here to prevent issues with requesting from site too frequently
				sleep(sleep_time)

		if loghtml:
			log_errors_to_file(caught_exceptions, caught_exceptions_warn)
		else:
			if len(caught_exceptions)==1:
				raise caught_exceptions[0]
			elif len(caught_exceptions)>0:
				msg = f"{len(caught_exceptions)} URL errors encountered:\n"
				for e in caught_exceptions:
					msg += "\t" + e.args[0] + "\n"
				raise OPD_MultipleErrors(msg)

			for e in caught_exceptions_warn:
				warnings.warn(str(e))

	
	def test_get_agencies(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]
			
		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue

			if is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Agency"] != MULTI:
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i} of {len(datasets)}: {srcName} {table_print} table")

				agencies = src.get_agencies(datasets.iloc[i]["TableType"], year=datasets.iloc[i]["Year"])

				if datasets.iloc[i]["Agency"] != MULTI:
					assert [datasets.iloc[i]["Agency"]] == agencies
				else:
					assert len(agencies) > 0

				# Adding a pause here to prevent issues with requesting from site too frequently
				sleep(sleep_time)


	def test_get_agencies_name_match(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		get_datasets(csvfile)

		src = data.Source("Virginia")

		agencies = src.get_agencies(partial_name="Arlington")

		assert len(agencies) == 2
				
				
	def test_agency_filter(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		get_datasets(csvfile)
		src = data.Source("Virginia")
		agency="Fairfax County Police Department"
		# For speed, set private limit parameter so that only a single entry is requested
		src._Source__limit = 100
		table = src.load_from_url(2021, agency=agency)
		
		assert len(table.table)==100
		assert table.table[table._agency_field].nunique()==1
		assert table.table.iloc[0][table._agency_field] == agency

	def test_to_csv(self, csvfile, source, last, skip, loghtml):
		src = data.Source("Virginia")
		get_datasets(csvfile)
		agency="Fairfax County Police Department"
		src._Source__limit = 100
		year = 2021
		table = src.load_from_url(2021, agency=agency)

		table.to_csv()

		filename = table.get_csv_filename()
		assert os.path.exists(filename)

		# Load table back in
		src.load_from_csv(year, agency=agency)

		os.remove(filename)

	
	@pytest.mark.slow(reason="This is a slow test that should be run before a major commit.")
	def test_load_year(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		# Test that filtering for a year works at the boundaries
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]
			
		caught_exceptions = []
		caught_exceptions_warn = []
		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue
			if is_filterable(datasets.iloc[i]["DataType"]) and datasets.iloc[i]["Year"] == MULTI:
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]

				if datasets.iloc[i]["Agency"] == MULTI and \
					srcName == "Virginia":
					# Reduce size of data load by filtering by agency
					agency = "Henrico Police Department"
				else:
					agency = None

				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i} of {len(datasets)}: {srcName} {table_print} table")

				src = data.Source(srcName, state=state)

				try:
					years = src.get_years(datasets.iloc[i]["TableType"])
				except warn_errors as e:
					e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"])
					caught_exceptions_warn.append(e)
					continue
				except (OPD_TooManyRequestsError, OPD_arcgisAuthInfoError) as e:
					# Catch exceptions related to URLs not functioning
					e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"])
					caught_exceptions.append(e)
					continue
				except:
					raise

				# Adding a pause here to prevent issues with requesting from site too frequently
				sleep(sleep_time)

				for y in src.datasets[src.datasets["TableType"] == datasets.iloc[i]["TableType"]]["Year"]:
					if y != MULTI:
						years.remove(y)

				if len(years)>1:
					# It is preferred to to not use first or last year that start and stop of year are correct
					year = years[-2]
				else:
					year = years[0]

				print(f"Testing for year {year}")

				table = src.load_from_url(year, datasets.iloc[i]["TableType"], 
										agency=agency)

				sleep(sleep_time)

				dts = table.table[datasets.iloc[i]["date_field"]]
				dts = dts.sort_values(ignore_index=True)

				all_years = dts.dt.year.unique().tolist()
				
				try:
					assert len(all_years) == 1
				except AssertionError as e:
					# The Fayetteville Traffic stops data filters appears to local time but return
					# UTC time which results in times that are 5 hours into the next year (EST is UTC-5).
					# Until this is solved more elegantly, removing years between {year}-01-01 00:00:00
					# and {year}-01-01 05:00:00
					dts = dts[(dts < f"{year+1}-01-01 00:00:00") | (dts > f"{year+1}-01-01 05:00:00")]
					all_years = dts.dt.year.unique().tolist()
					assert len(all_years) == 1
				except:
					raise(e)
				assert all_years[0] == year

				start_date = str(year-1) + "-12-29"
				stop_date = datetime.strftime(dts.iloc[0]+timedelta(days=1), "%Y-%m-%d")

				table_start = src.load_from_url([start_date, stop_date], datasets.iloc[i]["TableType"], 
												agency=agency)
				sleep(sleep_time)
				dts_start = table_start.table[datasets.iloc[i]["date_field"]]

				dts_start = dts_start.sort_values(ignore_index=True)

				# If this isn't true then the stop date is too early
				assert dts_start.iloc[-1].year == year

				# Find first value date in year
				dts_start = dts_start[dts_start.dt.year == year]
				try:
					assert dts.iloc[0] == dts_start.iloc[0]
				except AssertionError as e:
					# See comments in above try/except
					dts_start = dts_start[(dts_start < f"{year}-01-01 00:00:00") | (dts_start > f"{year}-01-01 05:00:00")]
					assert dts.iloc[0] == dts_start.iloc[0]
				except:
					raise(e)

				start_date = datetime.strftime(dts.iloc[-1]-timedelta(days=1), "%Y-%m-%d")
				stop_date  = str(year+1) + "-01-10"  

				table_stop = src.load_from_url([start_date, stop_date], datasets.iloc[i]["TableType"], 
												agency=agency)
				sleep(sleep_time)
				dts_stop = table_stop.table[datasets.iloc[i]["date_field"]]

				dts_stop = dts_stop.sort_values(ignore_index=True)

				# If this isn't true then the start date is too late
				assert dts_stop.iloc[0].year == year

				# Find last value date in year
				dts_stop = dts_stop[dts_stop.dt.year == year]
				assert dts.iloc[-1] == dts_stop.iloc[-1]

		if loghtml:
			log_errors_to_file(caught_exceptions, caught_exceptions_warn)
		else:
			if len(caught_exceptions)==1:
				raise caught_exceptions[0]
			elif len(caught_exceptions)>0:
				msg = f"{len(caught_exceptions)} URL errors encountered:\n"
				for e in caught_exceptions:
					msg += "\t" + e.args[0] + "\n"
				raise OPD_MultipleErrors(msg)

			for e in caught_exceptions_warn:
				warnings.warn(str(e))



	@pytest.mark.slow(reason="This is a slow test and should be run before a major commit.")
	def test_source_download_not_limitable(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]
			
		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue
			if not can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
				if is_stanford(datasets.iloc[i]["URL"]):
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
				table = src.load_from_url(year, table_type)

				sleep(sleep_time)

				assert len(table.table)>1
				if not pd.isnull(datasets.iloc[i]["date_field"]):
					assert datasets.iloc[i]["date_field"] in table.table
					#assuming a Pandas string dtype('O').name = object is okay too
					assert (table.table[datasets.iloc[i]["date_field"]].dtype.name in ['datetime64[ns]', 'datetime64[ms]'])
				if not pd.isnull(datasets.iloc[i]["agency_field"]):
					assert datasets.iloc[i]["agency_field"] in table.table


def can_be_limited(table_type, url):
	if (table_type == "CSV" and ".zip" in url):
		return False
	elif (table_type == "ArcGIS" or table_type == "Socrata" or table_type == "CSV"):
		return True
	else:
		raise ValueError("Unknown table type")


def is_filterable(table_type):
	if table_type == "CSV":
		return False
	elif (table_type == "ArcGIS" or table_type == "Socrata" ):
		return True
	else:
		raise ValueError("Unknown table type")

def is_stanford(url):
	return "stanford.edu" in url

def log_errors_to_file(*args):
	if not os.path.exists(log_folder):
		os.mkdir(log_folder)

	filename = os.path.join(log_folder, log_filename)

	if os.path.exists(filename):
		perm = "r+"
	else:
		perm = "w"

	with open(filename, perm) as f:
		for x in args:
			for e in x:
				new_line = ', '.join([str(x) for x in e.args])
				skip = False
				if perm == "r+":
					for line in f:
						if new_line in line or line in new_line:
							skip = True
							break

				if not skip:
					f.write(new_line)
					f.write("\n")

if __name__ == "__main__":
	# For testing
	tp = TestData()
	# (self, csvfile, source, last, skip, loghtml)
	tp.test_source_download_limitable(r"..\opd-data\opd_source_table.csv", "Louisville", None, None, None) 
