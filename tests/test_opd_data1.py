import pytest
import requests

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, datasets, data_loaders
from openpolicedata.defs import MULTI
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
from datetime import datetime
import pandas as pd
from time import sleep
import warnings
import os

log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError)

def get_datasets(csvfile):
    if csvfile != None:
        datasets.datasets = datasets._build(csvfile)

    return datasets.query()

class TestData:
	def test_source_urls(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]

		pause_time = 5
		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue

			srcName = datasets.iloc[i]["SourceName"]
			table_print = datasets.iloc[i]["TableType"]
			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			url = datasets.iloc[i]["URL"]
			print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table with url {url}")

			for k in range(2):  # Attempting twice in case, sites reject due to too many requests in a row
				url = datasets.iloc[i]["URL"]
				try:
					r = requests.head(url)
				except requests.exceptions.MissingSchema:
					if url[0:4] != "http":
						https = "https://"
						url = https + url
						try:
							r = requests.head(url)
						except:
							if k==0:
								sleep(pause_time)
								continue
							else:
								raise
					else:
						if k==0:
							sleep(pause_time)
							continue
						else:
							raise
				except:
					if k==0:
						sleep(pause_time)
						continue
					else:
						raise

				# 200 is success
				# 301 is moved permanently. This is most likely NYC. In this case, the main site has moved but the datasets have not
				if r.status_code != 200 and r.status_code != 301:
					r = requests.get(url)
					if r.status_code != 200 and r.status_code != 301:
						if k==0:
							sleep(pause_time)
							continue
						else:
							raise ValueError(f"Status code for {url} is {r.status_code}")

				break


	def test_check_version(self, csvfile, source, last, skip, loghtml):
		ds = get_datasets(csvfile).iloc[0]
		# Set min_version to create error
		ds["min_version"] = "-1"
		with pytest.raises(OPD_FutureError):
			data._check_version(ds)

		ds["min_version"] = "100000.0"
		with pytest.raises(OPD_MinVersionError):
			data._check_version(ds)

		# These should pass
		ds["min_version"] = "0.0"
		data._check_version(ds)
		ds["min_version"] = pd.NA
		data._check_version(ds)


	def test_get_count(self, csvfile, source, last, skip, loghtml):

		print("Testing Socrata source")
		src = data.Source("Virginia")
		loader = data_loaders.Socrata(src.datasets.iloc[0]["URL"], src.datasets.iloc[0]["dataset_id"], date_field=src.datasets.iloc[0]["date_field"])  
		year = 2021
		assert loader.get_count(year=year) == src.get_count(year)
		count = src.get_count([2020,2022])
		year = [2020,2022]
		assert loader.get_count(year=year) == src.get_count(year)

		agency = "Arlington County Police Department"
		opt_filter = src.datasets.iloc[0]["agency_field"] + " LIKE '%" + agency + "%'"
		year = 2021
		assert src.get_count(year, agency=agency) == loader.get_count(year=year, opt_filter=opt_filter)

		print("Testing ArcGIS source")
		src = data.Source("Charlotte-Mecklenburg")
		count = src.get_count(table_type="EMPLOYEE")

		url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16/"
		gis = data_loaders.Arcgis(url)
		assert count == gis.get_count()

		print("Testing CSV source")
		src = data.Source("San Diego")

		url = "https://seshat.datasd.org/crb/crb_cases_fy2020_datasd.csv"
		loader = data_loaders.Csv(url, date_field="INCIDENT_DATE")

		year = 2020
		count = src.get_count(year, table_type="COMPLAINTS", force=True)
			
		assert loader.get_count() == count


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
				print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

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
				sleep(0.1)

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
	csvfile = r"..\opd-data\opd_source_table.csv"
	# csvfile = None
	last = None
	# last = 493-363+1
	# tp.test_source_urls(csvfile, None, last, None, None) 
	# tp.test_check_version(csvfile, None, last, None, None) 
	tp.test_get_count(csvfile, None, last, None, None)
	tp.test_get_years(csvfile, None, last, None, None) 
