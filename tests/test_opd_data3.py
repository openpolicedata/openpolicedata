import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, data_loaders
from openpolicedata import datasets
from openpolicedata.defs import MULTI, DataType
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError, \
	DateFilterException
import random
from datetime import datetime
import pandas as pd
from time import sleep
import warnings
import os
import re

from test_utils import check_for_dataset

# Set Arcgis data loader to validate queries with arcgis package if installed
data_loaders._verify_arcgis = True

sleep_time = 0.1
log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

outages_file = os.path.join("..","opd-data","outages.csv")
# if has_outages:=os.path.exists(outages_file):
has_outages=os.path.exists(outages_file)
if has_outages:
	outages = pd.read_csv(outages_file)
else:
	try:
		outages = pd.read_csv('https://raw.githubusercontent.com/openpolicedata/opd-data/main/outages.csv')
		has_outages = True
	except:
		pass
	
warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError)

def test_bloomington_citations():
	if check_for_dataset('Bloomington', 'CITATIONS'):
		src = data.Source("Bloomington")
		count = src.get_count('CITATIONS', MULTI)

		match = src.datasets['TableType']=='CITATIONS'
		assert match.sum()==1
		dataset = src.datasets[match].iloc[0]
		loader = data_loaders.Socrata(dataset['URL'], dataset['dataset_id'])
		df = loader.load()
		assert len(df) == count
		dates = df[dataset['date_field']]

		count_by_year = []
		for y in range(2016, 2021):
			count_by_year.append(src.get_count('CITATIONS', y))
			matches_YYYY = dates.str.startswith(f"{y}-")
			dates = dates.drop(index=matches_YYYY[matches_YYYY].index)
			matches_YY = dates.str.match(r'\d{1,2}/\d{1,2}/'+str(y)[2:])
			dates = dates.drop(index=matches_YY[matches_YY].index)
			assert count_by_year[-1] == matches_YYYY.sum() + matches_YY.sum()

		assert count==sum(count_by_year)

@pytest.mark.slow(reason="This is a slow test that should be run before a major commit.")
def test_load_year(datasets, source, start_idx, skip, loghtml, query={}):
	max_count = 1e5
		
	caught_exceptions = []
	caught_exceptions_warn = []
	already_run = []
	for i in range(len(datasets)):
		if skip != None and datasets.iloc[i]["SourceName"] in skip:
			continue
		if i < start_idx:
			continue
		if source != None and datasets.iloc[i]["SourceName"] != source:
			continue

		if is_stanford(datasets.iloc[i]["URL"]):
			# There are a lot of data sets from Stanford, no need to run them all
			# Just run approximately a small percentage
			rnd = random.uniform(0,1)
			if rnd>0.05:
				continue

		match = True
		for k,v in query.items():
			if datasets.iloc[i][k]!=v:
				match = False
				break
		if not match:
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

		now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
		print(f"{now} Testing {i} of {len(datasets)-1}: {srcName} {table_print} table")

		src = data.Source(srcName, state=state)

		# Handle cases where URL is required to disambiguate requested dataset
		ds_filter, _ = src._Source__filter_for_source(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], None, errors=False)
		ds_input = datasets.iloc[[i]] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None

		if ds_input is None:
			already_run.append(unique_id)

		try:
			try:
				years = src.get_years(datasets.iloc[i]["TableType"], datasets=ds_input)
			except ValueError as e:
				if len(e.args)>0 and "Extracting the years" in e.args[0]:
					# Just test reading in the table and continue
					table = src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], 
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
				table = src.load(datasets.iloc[i]["TableType"], year, 
										agency=agency, pbar=False, 
										sortby="date",
										nrows=max_count if datasets.iloc[i]["DataType"] not in ["CSV","Excel"] else None)
			except OPD_FutureError as e:
				future_error = True
				break
			except (OPD_DataUnavailableError, OPD_SocrataHTTPError) as e:
				caught_exceptions_warn.append(e)
				tables.append(None)
				continue
			except:
				raise

			sleep(sleep_time)

			if len(table.table)==0:
				# Ensure count should have been 0
				count = src.get_count(datasets.iloc[i]["TableType"], year, agency=agency, force=True)
				if count!=0:
					raise ValueError(f"Expected data for year {year} but received none")
				
				# There may not be any data for the year requested.
				for y in years_orig:
					if y not in years:
						count = src.get_count(datasets.iloc[i]["TableType"], y, agency=agency, force=True)
						if count>0:
							years = [x if x!=year else y for x in years]
							table = src.load(datasets.iloc[i]["TableType"], y, 
										agency=agency, pbar=False, 
										sortby="date",
										nrows=max_count if datasets.iloc[i]["DataType"] not in ["CSV","Excel"] else None)
							break
				else:
					if len(table.table)==0 and has_outages and \
						(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
						pass
					else:
						raise ValueError("Unable to find data for any year")

			tables.append(table)

		if future_error:
			continue

		for year in years:
			print(f"Testing for year {year}")

			table = tables[years.index(year)]
			if table is None: # Data could not be loaded
				continue

			if len(table.table)==0 and has_outages and \
				(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
				caught_exceptions_warn.append(f'Outage continues for {str(datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]])}')
				continue
			else:
				assert len(table.table)>0

			if table.date_field == None or datasets.iloc[i]["DataType"]==DataType.EXCEL.value or \
				table.date_field.lower()=="year":
				continue

			dts = table.table[table.date_field]
			# Remove all non-datestamps
			dts = dts[dts.apply(lambda x: isinstance(x,pd.Timestamp) or isinstance(x,pd.Period))].convert_dtypes()
			try:
				dts = dts.sort_values(ignore_index=True)
			except TypeError as e:
				if re.search(r"not supported between instances of '(Timestamp|Period)' and '(Timestamp|Period)'", str(e)):
					dts = dts[dts.apply(lambda x: isinstance(x,pd.Timestamp))]
					dts = dts.sort_values(ignore_index=True)
				else:
					raise

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

			if "month" in table.date_field.lower() or "year" in table.date_field.lower() or "yr" in table.date_field.lower():
				# Cannot currently filter only by month/year
				continue
			
			start_date = str(year-1) + "-12-29"
			stop_date = datetime.strftime(dts.iloc[0], "%Y-%m-%d")

			try:
				table_start = src.load(datasets.iloc[i]["TableType"], [start_date, stop_date], 
												agency=agency, pbar=False)
			except ValueError as e:
				if str(e).startswith('Year range cannot contain the year corresponding to a single year dataset'):
					start_date  = str(year) + "-01-01"
					table_start = src.load(datasets.iloc[i]["TableType"], [start_date, stop_date], 
												agency=agency, pbar=False)
				else:
					raise
			except DateFilterException as e:
				continue

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

			start_date = datetime.strftime(dts.iloc[-1], "%Y-%m-%d")
			stop_date  = str(year+1) + "-01-10"  

			try:
				table_stop = src.load(datasets.iloc[i]["TableType"], [start_date, stop_date], 
												agency=agency, pbar=False)
			except ValueError as e:
				if str(e).startswith('There is more than one source matching') or \
					str(e).startswith('Year range cannot contain the year corresponding to a single year dataset'):
					stop_date  = str(year) + "-12-31"  
					table_stop = src.load(datasets.iloc[i]["TableType"], [start_date, stop_date], 
												agency=agency, pbar=False)
				else:
					raise
			sleep(sleep_time)
			dts_stop = table_stop.table[table.date_field]

			dts_stop = dts_stop[dts_stop.apply(lambda x: not isinstance(x,str))]
			try:
				dts_stop = dts_stop.sort_values(ignore_index=True)
			except TypeError as e:
				dts_stop = dts_stop[dts_stop.apply(lambda x: isinstance(x,pd.Timestamp))]
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


def test_source_download_not_limitable(datasets, source, start_idx, skip, query={}):		
	for i in range(len(datasets)):
		if skip != None and datasets.iloc[i]["SourceName"] in skip:
			continue
		if i < start_idx:
			continue
		if source != None and datasets.iloc[i]["SourceName"] != source:
			continue

		if datasets.iloc[i]["DataType"] == DataType.CSV and ".zip" in datasets.iloc[i]["URL"]:
			if is_stanford(datasets.iloc[i]["URL"]):
				# There are a lot of data sets from Stanford, no need to run them all
				# Just run approximately 10%
				rnd = random.uniform(0,1)
				if rnd>0.05:
					continue

			match = True
			for k,v in query.items():
				if datasets.iloc[i][k]!=v:
					match = False
					break
			if not match:
				continue

			srcName = datasets.iloc[i]["SourceName"]
			state = datasets.iloc[i]["State"]
			src = data.Source(srcName, state=state)

			year = datasets.iloc[i]["Year"]
			table_type = datasets.iloc[i]["TableType"]

			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			print(f"{now} Testing {i+1} of {len(datasets)}: {srcName}, {state} {table_type} table for {year}")
			try:
				table = src.load(table_type, year, pbar=False)
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
	from test_utils import get_datasets
	# For testing
	use_changed_rows = False
	csvfile = None
	# csvfile = os.path.join('..','opd-data', 'opd_source_table.csv')
	start_idx = 404
	skip = None
	# skip = "Sacramento,Beloit,Rutland"
	source = None
	# source = "Asheville"
	query = {}
	# query = {'DataType':'CSV'}

	datasets = get_datasets(csvfile, use_changed_rows)

	test_load_year(datasets, source, start_idx, skip, False, query=query) 
	start_idx = 0
	test_source_download_not_limitable(datasets, source, start_idx, skip) 
