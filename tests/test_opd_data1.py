import pytest
import requests

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import _datasets
from openpolicedata import datasets_query
from openpolicedata.defs import MULTI
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
from datetime import datetime
import pandas as pd
from time import sleep
import warnings
import os

sleep_time = 0.1
log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
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

			src.datasets.loc[k, "min_version"] = "100000.0"
			with pytest.raises(OPD_MinVersionError):
				src.get_years(src.datasets.loc[k, "TableType"])

			# These should pass
			src.datasets.loc[k, "min_version"] = "0.0"
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

			src.datasets.loc[k, "min_version"] = "1000000.0"
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

			src.datasets.loc[k, "min_version"] = "1000000.0"
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
