import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, data_loaders
from openpolicedata import datasets
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

# Set Arcgis data loader to validate queries with arcgis package if installed
data_loaders._verify_arcgis = True

sleep_time = 0.1
log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError)

def get_datasets(csvfile):
    if csvfile != None:
        datasets.datasets = datasets._build(csvfile)

    return datasets.datasets

class TestData:
	@pytest.mark.slow(reason="This is a slow test that should be run before a major commit.")
	def test_load_year(self, csvfile, source, last, skip, loghtml):
		max_count = 1e5
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		# Test that filtering for a year works at the boundaries
		if skip != None:
			skip = skip.split(",")
			skip = [x.strip() for x in skip]
			
		caught_exceptions = []
		caught_exceptions_warn = []
		already_run = []
		for i in range(len(datasets)):
			if skip != None and datasets.iloc[i]["SourceName"] in skip:
				continue
			if i < len(datasets) - last:
				continue
			if source != None and datasets.iloc[i]["SourceName"] != source:
				continue

			if is_stanford(datasets.iloc[i]["URL"]):
				# There are a lot of data sets from Stanford, no need to run them all
				# Just run approximately a small percentage
				rnd = random.uniform(0,1)
				if rnd>0.05:
					continue

			srcName = datasets.iloc[i]["SourceName"]
			state = datasets.iloc[i]["State"]

			if datasets.iloc[i]["Agency"] == MULTI and \
				srcName == "Virginia":
				# Reduce size of data load by filtering by agency
				agency = "Henrico Police Department"
			else:
				agency = None

			table_print = datasets.iloc[i]["TableType"]

			unique_id = [srcName, state, datasets.iloc[i]["Agency"], table_print]
			if unique_id in already_run:
				continue
			already_run.append(unique_id)

			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

			src = data.Source(srcName, state=state)

			try:
				try:
					years = src.get_years(datasets.iloc[i]["TableType"])
				except ValueError as e:
					if len(e.args)>0 and "Extracting the years" in e.args[0]:
						# Just test reading in the table and continue
						table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"], 
										agency=agency, pbar=False)
						continue
					else:
						raise
				except:
					raise
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

			years_orig = years.copy()
			for y in src.datasets[src.datasets["TableType"] == datasets.iloc[i]["TableType"]]["Year"]:
				if y != MULTI:
					years.remove(y)

			if len(years)==0:
				# Each datasets is annual
				# Run 1st and last year
				multi_case = False
				years = [min(years_orig)]
				if len(years_orig)>1:
					years.append(max(years_orig))
			else:
				multi_case = True
				if len(years)>1:
					# It is preferred to to not use first or last year that start and stop of year are correct
					years = years[-2:-1]
				else:
					years = years[:1]

			tables = []
			future_error = False
			for year in years:
				try:
					table = src.load_from_url(year, datasets.iloc[i]["TableType"], 
											agency=agency, pbar=False, 
											nrows=max_count if datasets.iloc[i]["DataType"] not in ["CSV","Excel"] else None)
				except OPD_FutureError as e:
					future_error = True
					break
				except:
					raise

				sleep(sleep_time)

				if len(table.table)==0:
					# Ensure count should have been 0
					count = src.get_count(year, datasets.iloc[i]["TableType"], agency=agency)
					if count!=0:
						raise ValueError(f"Expected data for year {year} but received none")

					# There may not be any data for the year requested. First and last year are most likely to have data
					if years_orig[-1] not in years:
						years = [x if x!=year else years_orig[-1] for x in years]
						table = src.load_from_url(years_orig[-1], datasets.iloc[i]["TableType"], 
											agency=agency, pbar=False, 
											nrows=max_count if datasets.iloc[i]["DataType"] not in ["CSV","Excel"] else None)
					elif years_orig[0] not in years:
						years = [x if x!=year else years_orig[0] for x in years]
						table = src.load_from_url(years_orig[0], datasets.iloc[i]["TableType"], 
											agency=agency, pbar=False, 
											nrows=max_count if datasets.iloc[i]["DataType"] not in ["CSV","Excel"] else None)

				tables.append(table)

			if future_error:
				continue

			for year in years:
				print(f"Testing for year {year}")

				table = tables[years.index(year)]

				assert len(table.table)>0

				if table.date_field == None or datasets.iloc[i]["DataType"]==DataType.EXCEL.value or \
					table.date_field.lower()=="year":
					continue

				dts = table.table[table.date_field]
				# Remove all non-datestamps
				dts = dts[dts.apply(lambda x: isinstance(x,pd._libs.tslibs.timestamps.Timestamp))].convert_dtypes()
				dts = dts.sort_values(ignore_index=True)

				all_years = dts.dt.year.unique().tolist()
				
				try:
					assert len(all_years) == 1
				except AssertionError as e:
					# Some datasets filter by local time but return
					# UTC time
					# Until this is solved more elegantly, removing years between {year}-01-01 00:00:00
					# and {year}-01-01 08:00:00
					dts = dts[(dts < f"{year+1}-01-01 00:00:00") | (dts > f"{year+1}-01-01 08:00:00")]
					all_years = dts.dt.year.unique().tolist()
					assert len(all_years) == 1
				except:
					raise(e)
				assert all_years[0] == year

				if not multi_case:
					continue

				if "month" in table.date_field.lower():
					# Cannot currently filter only by month/year
					continue
				
				start_date = str(year-1) + "-12-29"
				stop_date = datetime.strftime(dts.iloc[0]+timedelta(days=1), "%Y-%m-%d")

				try:
					table_start = src.load_from_url([start_date, stop_date], datasets.iloc[i]["TableType"], 
													agency=agency, pbar=False)
				except ValueError as e:
					if len(e.args)>0 and e.args[0]=="Currently unable to handle non-year inputs":
						# The format of the date field does not allow for filtering by date
						continue
					else:
						raise
				except:
					raise

				sleep(sleep_time)
				dts_start = table_start.table[table.date_field]
				dts_start = dts_start[dts_start.apply(lambda x: isinstance(x,pd._libs.tslibs.timestamps.Timestamp))].convert_dtypes()
				dts_start = dts_start.sort_values(ignore_index=True, na_position="first")

				# If this isn't true then the stop date is too early
				assert dts_start.iloc[-1].year == year

				# Find first value date in year
				dts_start = dts_start[dts_start.dt.year == year]
				try:
					assert dts.iloc[0] == dts_start.iloc[0]
				except AssertionError as e:
					# See comments in above try/except
					assert dts.iloc[0].tz_localize(None) <= datetime.strptime(f"{year}-01-01 08:00:00", "%Y-%m-%d %H:%M:%S")
				except:
					raise(e)
				
				if len(table.table) == max_count:
					# Whole dataset was not read. Don't compare to latest data in the year
					continue

				start_date = datetime.strftime(dts.iloc[-1]-timedelta(days=1), "%Y-%m-%d")
				stop_date  = str(year+1) + "-01-10"  

				table_stop = src.load_from_url([start_date, stop_date], datasets.iloc[i]["TableType"], 
												agency=agency, pbar=False)
				sleep(sleep_time)
				dts_stop = table_stop.table[table.date_field]

				dts_stop = dts_stop[dts_stop.apply(lambda x: not isinstance(x,str))]
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
				print(f"{now} Testing {i+1} of {len(datasets)}: {srcName}, {state} {table_type} table for {year}")
				try:
					table = src.load_from_url(year, table_type, pbar=False)
				except OPD_FutureError:
					continue
				except:
					raise

				sleep(sleep_time)

				assert len(table.table)>1
				if not pd.isnull(table.date_field):
					assert table.date_field in table.table
					#assuming a Pandas string dtype('O').name = object is okay too
					assert (table.table[table.date_field].dtype.name in ['datetime64[ns]', 'datetime64[ms]'])
				if not pd.isnull(datasets.iloc[i]["agency_field"]):
					assert datasets.iloc[i]["agency_field"] in table.table


def can_be_limited(data_type, url):
	data_type = DataType(data_type)
	if (data_type == DataType.CSV and ".zip" in url):
		return False
	elif data_type in [DataType.ArcGIS, DataType.SOCRATA, DataType.CSV, DataType.EXCEL, DataType.CARTO]:
		return True
	else:
		raise ValueError("Unknown table type")


def is_filterable(data_type):
	data_type = DataType(data_type)
	if data_type in [DataType.CSV, DataType.EXCEL]:
		return False
	elif data_type in [DataType.ArcGIS, DataType.SOCRATA, DataType.CARTO]:
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
	csvfile = None
	csvfile = r"..\opd-data\opd_source_table.csv"
	last = None
	last = 876-258+1
	skip = None
	skip = "Corona,Bloomington"
	source = None
	# source = "Detroit"
	tp.test_load_year(csvfile, source, last, skip, None)
	tp.test_source_download_not_limitable(csvfile, source, last, skip, None)
