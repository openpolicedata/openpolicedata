if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import datasets
from openpolicedata.defs import MULTI, DataType
import openpolicedata as opd
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
from datetime import datetime
from time import sleep
import warnings
import os
import pandas as pd
import pytest

from test_utils import check_for_dataset

sleep_time = 0.1
log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

# Set Arcgis data loader to validate queries with arcgis package if installed
opd.data_loaders._verify_arcgis = True

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

multi_states = ['Connecticut','New York','Virginia']
multi_tables = ['TRAFFIC STOPS','TRAFFIC CITATIONS','STOPS']
multi_years = [MULTI,MULTI,MULTI]
multi_partial = ["Hartford",'Buffalo',"Arlington"]

@pytest.fixture(scope='module')
def buffalo_data():
	if check_for_dataset('New York', 'TRAFFIC CITATIONS'):
		src = data.Source("New York")
		agency="BUFFALO POLICE DEPT"
		return  src.load('TRAFFIC CITATIONS', 2021, agency=agency, pbar=False, nrows=100)
	

def test_get_years(datasets, source, start_idx, skip, loghtml, query={}):
	caught_exceptions = []
	caught_exceptions_warn = []

	already_ran = []
	last_source = None
	for i in range(len(datasets)):
		if source != None and datasets.iloc[i]["SourceName"] != source:
			continue
		if skip != None and datasets.iloc[i]["SourceName"] in skip:
			continue
		if i < start_idx:
			continue
		if is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Year"] != MULTI or \
			datasets.iloc[i]["DataType"] == DataType.EXCEL.value:  # If Excel, we can possibly check
			srcName = datasets.iloc[i]["SourceName"]
			state = datasets.iloc[i]["State"]

			match = True
			for k,v in query.items():
				if datasets.iloc[i][k]!=v:
					match = False
					break
			if not match:
				continue

			if (srcName, state, datasets.iloc[i]["TableType"]) in already_ran:
				continue

			src = data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

			table_print = datasets.iloc[i]["TableType"]
			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

			if datasets.iloc[i]["DataType"] == DataType.EXCEL.value:
				if type(datasets.iloc[i]["dataset_id"]) in [list, dict]:
					# Multi-dataset table
					if pd.isnull(datasets.iloc[i]['date_field']):
						continue
				else:
					try:
						loader = opd.data_loaders.Excel(datasets.iloc[i]["URL"], data_set=datasets.iloc[i]["dataset_id"])
					except OPD_DataUnavailableError:
						continue
					has_year_sheets = loader._Excel__get_sheets()[1]
					if not has_year_sheets:
						continue				

			already_ran.append((srcName, state, datasets.iloc[i]["TableType"]))

			if srcName == last_source:
				sleep(num_sources*0.1) # Sleep for a bit to not hit the same site repeatedly too hard
				num_sources +=1
			else:
				num_sources = 1
			last_source = srcName

			try:
				years = src.get_years(datasets.iloc[i]["TableType"], force=True)
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

			if len(years)==0 and has_outages and \
				(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
				caught_exceptions_warn.append(f'Outage continues for {str(datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]])}')
				continue

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


def test_get_agencies(datasets, source, start_idx, skip):
		
	for i in range(len(datasets)):
		if skip != None and datasets.iloc[i]["SourceName"] in skip:
			continue
		if i < start_idx:
			continue
		if source != None and datasets.iloc[i]["SourceName"] != source:
			continue

		if is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Agency"] != MULTI:
			srcName = datasets.iloc[i]["SourceName"]
			state = datasets.iloc[i]["State"]
			src = data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

			table_print = datasets.iloc[i]["TableType"]
			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

			try:
				agencies = src.get_agencies(datasets.iloc[i]["TableType"], year=datasets.iloc[i]["Year"])
			except ValueError as e:
				if 'Inputs must filter for a single source' in str(e):
					agencies = src.get_agencies(datasets.iloc[i]["TableType"], year=datasets.iloc[i]["Year"], 
								 url=datasets.iloc[i]["URL"], id=datasets.iloc[i]["dataset_id"])
				else:
					raise
			except OPD_MinVersionError: continue
			except: raise

			if datasets.iloc[i]["Agency"] != MULTI:
				assert [datasets.iloc[i]["Agency"]] == agencies
			else:
				assert len(agencies) > 0

			# Adding a pause here to prevent issues with requesting from site too frequently
			sleep(sleep_time)


def test_multi_agency_list(datasets):
	for i in range(len(datasets)):
		if is_filterable(datasets.iloc[i]["DataType"]) and datasets.iloc[i]["Agency"] == MULTI:
			assert any([datasets.iloc[i]['State']==x and datasets.iloc[i]['TableType']==y and datasets.iloc[i]['Year']==z for 
						x,y,z in zip(multi_states, multi_tables,multi_years)])


@pytest.mark.parametrize('state,table_type,year,partial',[(x,y,z,a) for x,y,z,a in zip(multi_states, multi_tables,multi_years,multi_partial)])
def test_get_agencies_name_match(state, table_type, year, partial):
	if check_for_dataset(state, table_type):
		src = data.Source(state)
		try:
			agencies = src.get_agencies(partial_name=partial, table_type=table_type, year=year)
		except OPD_MinVersionError:
			return
		except Exception as e:
			raise

		assert len(agencies) > 1
			
			
def test_agency_filter():
	if check_for_dataset('New York', 'TRAFFIC CITATIONS'):
		src = data.Source("New York")
		agency="BUFFALO POLICE DEPT"
		# For speed, set private limit parameter so that only a single entry is requested
		table = src.load('TRAFFIC CITATIONS', 2021, agency=agency, pbar=False, nrows=100)
		
		assert len(table.table)==100
		assert table.table[table.agency_field].nunique()==1
		assert table.table.iloc[0][table.agency_field] == agency


@pytest.mark.parametrize('save,fname,load',[('to_csv','get_csv_filename','load_from_csv'),
											('to_csv','get_csv_filename','load_csv'),
											('to_feather','get_feather_filename','load_feather'),
											('to_parquet','get_parquet_filename','load_parquet')])
def test_save_load(buffalo_data, save,fname,load):
	if check_for_dataset('New York', 'TRAFFIC CITATIONS'):
		src = data.Source("New York")

		getattr(buffalo_data, save)()

		filename = getattr(buffalo_data, fname)()
		assert os.path.exists(filename)

		try:
			# Load table back in
			getattr(src, load)(table_type=buffalo_data.table_type, year=buffalo_data.year, agency=buffalo_data.agency)
		except:
			raise
		finally:
			os.remove(filename)


@pytest.mark.parametrize('save,fname',[('to_feather','get_feather_filename'),
											('to_parquet','get_parquet_filename')])
def test_save_mixed_dtype_column(save,fname):
	if check_for_dataset('New York City', 'PEDESTRIAN STOPS'):
		src = data.Source("New York City")
		table = src.load('PEDESTRIAN STOPS', 2014, pbar=False)
	
		getattr(table, save)(mixed=True)

		filename = getattr(table, fname)()
		assert os.path.exists(filename)
		os.remove(filename)


def is_filterable(data_type):
	data_type = DataType(data_type)
	if data_type in [DataType.CSV, DataType.EXCEL, DataType.HTML]:
		return False
	elif data_type in [DataType.ArcGIS, DataType.SOCRATA, DataType.CARTO, DataType.CKAN, DataType.OPENDATASOFT]:
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
	from test_utils import get_datasets
	# For testing
	# (csvfile, source, last, skip, loghtml)

	use_changed_rows = False

	csvfile = None
	csvfile = os.path.join("..","opd-data","opd_source_table.csv")
	start_idx = 1321
	source = None
	# source = "Burlington"
	skip = None
	# skip = "Sacramento,Beloit,Rutland"
	
	datasets = get_datasets(csvfile, use_changed_rows)

	# skip = "Corona"
	# test_get_agencies(datasets, source, start_idx, skip)
	# test_agency_filter(datasets, None, None, skip, None)
	# test_to_csv(datasets, None, None, skip, None)
	test_get_years(datasets, source, start_idx, skip, None)
