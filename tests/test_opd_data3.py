import pytest
from packaging import version

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, data_loaders
from openpolicedata import __version__
from openpolicedata.defs import MULTI
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError, \
	DateFilterException
from datetime import datetime
import numpy as np
import pandas as pd
from time import sleep
import warnings
import os
import re
from urllib.parse import urlparse

import test_utils
from test_utils import check_for_dataset, user_request_skip, pop_dataset

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


@pytest.mark.veryslow(reason="This is a very slow test that should be run before a major commit.")
def test_load_year_annual_data(datasets, source, start_idx, skip, query={}):
	max_count = 1e5
		
	caught_exceptions = []
	caught_exceptions_warn = []
	already_run = []
	for i in range(len(datasets)):
		if user_request_skip(datasets, i, skip, start_idx, source, query, skip_zip=True):
			continue

		srcName, state, table_type, agency_ds = pop_dataset(datasets.iloc[i])
		agency = 'Henrico Police Department' if datasets.iloc[i]["Agency"] == MULTI and srcName == "Virginia" else None

		unique_id = [srcName, state, agency_ds, table_type]
		if unique_id in already_run:
			continue

		src = data.Source(srcName, state=state, agency=agency_ds)
		cur_datasets = src.datasets[src.datasets["TableType"] == table_type]
		# Remove datasets this version can't read
		cur_datasets = cur_datasets[cur_datasets['min_version'].apply(lambda x: pd.isnull(x) or (x!='-1' and version.parse(x)<=version.parse(__version__)))]
		cur_years = cur_datasets["Year"].tolist()
		multi_case = any(isinstance(x,str) for x in cur_years)

		if multi_case or len(cur_datasets)==0:
			continue

		now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
		print(f"{now} Testing {i} of {len(datasets)-1}: {srcName} {table_type} table")

		# Handle cases where URL is required to disambiguate requested dataset
		ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
		ds_input = datasets.iloc[[i]] if len(ds_filter)>1 else None

		if ds_input is None:
			already_run.append(unique_id)

		years_all = src.get_years(table_type, datasets=ds_input)
		years_run = list({min(years_all),max(years_all)})   # Run 1st and last year

		# Handle cases where URL is required to disambiguate requested dataset
		ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
		url = datasets.iloc[i]['URL'] if len(ds_filter)>1 else None
		id = datasets.iloc[i]['dataset_id'] if len(ds_filter)>1 else None

		for year in years_run:
			print(f"Testing for year {year}")
			table = test_utils.load_data(src, table_type, year, agency, 
									max_count if not test_utils.is_file(datasets, i) else None,
									url, id, caught_exceptions_warn)
			
			if table==None:
				continue

			sleep(sleep_time)

			if len(table.table)==0 and has_outages and \
				(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
				caught_exceptions_warn.append(f'Outage continues for {str(datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]])}')
				continue
			else:
				assert len(table.table)>0

			if table.date_field == None:
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
				# Ignore if annual datasets fail as this is indicative that the data contains bad dates
				assert len(all_years) == 1 or table.date == year
			except AssertionError as e:
				# Some datasets filter by local time but return
				# UTC time
				# Until this is solved more elegantly, removing years between {year}-01-01 00:00:00
				# and {year}-01-01 08:00:00
				dts = dts[(dts < f"{year+1}-01-01 00:00:00") | (dts > f"{year+1}-01-01 08:00:00")]
				all_years = dts.dt.year.unique().tolist()
				if len(all_years) != 1:
					m = (src.datasets['Year']==year) & (src.datasets['URL']==table.urls['data'])
					assert m.sum()==1
					# This is an annual dataset where perhaps some data is mistakenly from previous/next year
					dts = dts[(dts < f"{year-1}-12-31 00:00:00") | (dts > f"{year-1}-12-31 23:59:59")]
					all_years = dts.dt.year.unique().tolist()
					assert len(all_years)==1
			except:
				raise(e)
			assert year in all_years

			raise ValueError('Load for date range')

	if len(caught_exceptions)==1:
		raise caught_exceptions[0]
	elif len(caught_exceptions)>0:
		msg = f"{len(caught_exceptions)} URL errors encountered:\n"
		for e in caught_exceptions:
			msg += "\t" + e.args[0] + "\n"
		raise OPD_MultipleErrors(msg)

	for e in caught_exceptions_warn:
		warnings.warn(str(e))


@pytest.mark.veryslow(reason="This is a very slow test that should be run before a major commit.")
def test_load_year_none(datasets, source, start_idx, skip, query={}):
	raise NotImplementedError()


@pytest.mark.veryslow(reason="This is a very slow test that should be run before a major commit.")
def test_load_year_multiple(datasets, source, start_idx, skip, query={}):
	max_count = 1e5
		
	caught_exceptions = []
	caught_exceptions_warn = []
	already_run = []
	for i in range(len(datasets)):
		if user_request_skip(datasets, i, skip, start_idx, source, query, skip_zip=True):
			continue

		srcName, state, table_type, agency_ds = pop_dataset(datasets.iloc[i])
		agency = 'Henrico Police Department' if datasets.iloc[i]["Agency"] == MULTI and srcName == "Virginia" else None

		unique_id = [srcName, state, agency_ds, table_type]
		if unique_id in already_run:
			continue

		src = data.Source(srcName, state=state, agency=agency_ds)
		cur_datasets = src.datasets[src.datasets["TableType"] == table_type]
		# Remove datasets this version can't read
		cur_datasets = cur_datasets[cur_datasets['min_version'].apply(lambda x: pd.isnull(x) or (x!='-1' and version.parse(x)<=version.parse(__version__)))]
		cur_years = cur_datasets["Year"].tolist()
		multi_case = any(isinstance(x,str) for x in cur_years)

		if not multi_case or len(cur_datasets)==0 or cur_years == ['NONE']:
			continue

		now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
		print(f"{now} Testing {i} of {len(datasets)-1}: {srcName} {table_type} table")

		# Handle cases where URL is required to disambiguate requested dataset
		ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
		ds_input = datasets.iloc[[i]] if len(ds_filter)>1 else None

		if ds_input is None:
			already_run.append(unique_id)

		try:
			try:
				years_all = src.get_years(table_type, datasets=ds_input)
			except ValueError as e:
				raise  # Skipping below starting on 2/1/20
				# if len(e.args)>0 and "Extracting the years" in e.args[0]:
				# 	# Just test reading in the table and continue
				# 	table = src.load(table_type, datasets.iloc[i]["Year"], 
				# 					agency=agency, pbar=False)
				# 	continue
				# else:
				# 	raise
			except:
				raise
		except warn_errors as e:
			e.prepend(f"Iteration {i}", srcName, table_type)
			caught_exceptions_warn.append(e)
			continue
		except (OPD_TooManyRequestsError, OPD_arcgisAuthInfoError) as e:
			# Catch exceptions related to URLs not functioning
			e.prepend(f"Iteration {i}", srcName, table_type)
			caught_exceptions.append(e)
			continue
		except:
			raise

		# Adding a pause here to prevent issues with requesting from site too frequently
		sleep(sleep_time)

		years_avail = [y for y in years_all if y not in cur_years]  # List of years not in annual datasets
		years_run = years_avail[-2:-1] if len(years_avail)>1 else years_avail[:1]  # Do not use first or last year if possible

		# Handle cases where URL is required to disambiguate requested dataset
		ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
		url = datasets.iloc[i]['URL'] if len(ds_filter)>1 else None
		id = datasets.iloc[i]['dataset_id'] if len(ds_filter)>1 else None

		tables = {}
		for k in range(2):
			for year in years_run:
				table = test_utils.load_data(src, table_type, year, agency, 
									max_count if not test_utils.is_file(datasets, i) else None,
									url, id, caught_exceptions_warn)
				
				if table==None:
					continue

				sleep(sleep_time)

				if len(table.table)==0:
					# Ensure count should have been 0
					count = src.get_count(table_type, year, agency=agency, force=True)
					if count!=0:
						raise ValueError(f"Expected data for year {year} but received none")
					continue

				tables[year] = table
				if k>0:
					years_run = [year]
					break

			if k==0:
				if len(tables)==0:
					# No data found. Try to run some different years
					years_run = [y for y in years_avail if y not in years_run]
				else:
					break

		assert len(years_run)==1
		year = years_run[0]

		print(f"Testing for year {year}")

		table = tables[year]

		if len(table.table)==0 and has_outages and \
			(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
			caught_exceptions_warn.append(f'Outage continues for {str(datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]])}')
			continue
		else:
			assert len(table.table)>0

		if table.date_field == None:
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
			# Ignore if annual datasets fail as this is indicative that the data contains bad dates
			assert len(all_years) == 1 or table.date == year
		except AssertionError as e:
			# Some datasets filter by local time but return
			# UTC time
			# Until this is solved more elegantly, removing years between {year}-01-01 00:00:00
			# and {year}-01-01 08:00:00
			dts = dts[(dts < f"{year+1}-01-01 00:00:00") | (dts > f"{year+1}-01-01 08:00:00")]
			all_years = dts.dt.year.unique().tolist()
			if len(all_years) != 1:
				m = (src.datasets['Year']==year) & (src.datasets['URL']==table.urls['data'])
				assert m.sum()==1
				# This is an annual dataset where perhaps some data is mistakenly from previous/next year
				dts = dts[(dts < f"{year-1}-12-31 00:00:00") | (dts > f"{year-1}-12-31 23:59:59")]
				all_years = dts.dt.year.unique().tolist()
				assert len(all_years)==1
		except:
			raise(e)
		assert year in all_years

		if "month" in table.date_field.lower() or "year" in table.date_field.lower() or "yr" in table.date_field.lower() or \
			test_utils.is_file(datasets, i):
			# Cannot currently filter only by month/year and filtering by date for file will re-load entire dataset
			continue
		
		start_date = str(year-1) + "-12-29"
		stop_date = datetime.strftime(dts.iloc[0], "%Y-%m-%d")

		try:
			table_start = src.load(table_type, [start_date, stop_date], 
											agency=agency, pbar=False, url=url, id=id)
		except ValueError as e:
			if str(e).startswith('Year range cannot contain the year corresponding to a single year dataset'):
				start_date  = str(year) + "-01-01"
				table_start = src.load(table_type, [start_date, stop_date], 
											agency=agency, pbar=False, url=url, id=id)
			else:
				raise
		except (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_MinVersionError) as e:
			continue
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
			# This could be due to the API filtering in a different timezone than it return. Account for this by checking if first value is close to start of year
			assert dts.iloc[0].tz_localize(None) <= datetime.strptime(f"{year}-01-01 08:00:00", "%Y-%m-%d %H:%M:%S")
		except:
			raise(e)
		
		if len(table.table) == max_count:
			# Whole dataset was not read. Don't compare to latest data in the year
			continue

		start_date = datetime.strftime(dts.iloc[-1], "%Y-%m-%d")
		stop_date  = str(year+1) + "-01-10"  

		try:
			table_stop = src.load(table_type, [start_date, stop_date], 
											agency=agency, pbar=False, url=url, id=id)
		except ValueError as e:
			if str(e).startswith('There is more than one source matching') or \
				str(e).startswith('Year range cannot contain the year corresponding to a single year dataset'):
				stop_date  = str(year) + "-12-31"  
				table_stop = src.load(table_type, [start_date, stop_date], 
											agency=agency, pbar=False, url=url, id=id)
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

	if len(caught_exceptions)==1:
		raise caught_exceptions[0]
	elif len(caught_exceptions)>0:
		msg = f"{len(caught_exceptions)} URL errors encountered:\n"
		for e in caught_exceptions:
			msg += "\t" + e.args[0] + "\n"
		raise OPD_MultipleErrors(msg)

	for e in caught_exceptions_warn:
		warnings.warn(str(e))


if __name__ == "__main__":
	from test_utils import get_datasets
	# For testing
	use_changed_rows = False
	csvfile = None
	csvfile = os.path.join('..','opd-data', 'opd_source_table.csv')
	start_idx = 463
	skip = None
	# skip = "Sacramento,Beloit,Rutland"
	source = None
	# source = "Chicago"
	query = {}
	# query = {'DataType':'CSV'}

	datasets = get_datasets(csvfile, use_changed_rows)

	# test_load_year_annual_data(datasets, source, start_idx, skip, False, query=query) 
	test_load_year_multiple(datasets, source, start_idx, skip, False, query=query) 
	start_idx = 0
	# test_source_download_not_limitable(datasets, source, start_idx, skip) 
