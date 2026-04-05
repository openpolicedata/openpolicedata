import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, data_loaders
from openpolicedata.defs import DataType, TableType
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
import openpolicedata as opd
from datetime import datetime
import pandas as pd
from time import sleep
import warnings
import os

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_for_dataset, update_outages, user_request_skip

base_sleep_time = 0.1

# Set Arcgis data loader to validate queries with arcgis package if installed
data_loaders._verify_arcgis = True

log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

outages_file = os.path.join("..","opd-data","outages.csv")

warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError)


@pytest.mark.slow(reason="This test is slow to run and will be run last.")
def test_source_download_limitable(datasets, source, start_idx, skip, query={}):
	caught_exceptions = []
	caught_exceptions_warn = []
	last_source = None
	for i in range(len(datasets)):
		if user_request_skip(datasets, i, skip, start_idx, source, query):
			continue

		if can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
			srcName = datasets.iloc[i]["SourceName"]
			state = datasets.iloc[i]["State"]
			src = data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

			table_type = datasets.iloc[i]["TableType"]
			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			print(f"{now} Testing {i} of {len(datasets)-1}: {srcName} {table_type} table")

			limit_rows =  datasets.iloc[i]['DataType'] not in ['Excel','HTML'] and \
				(not isinstance(datasets.iloc[i]['dataset_id'], str) or ';' not in datasets.iloc[i]['dataset_id'])
			nrows = 20 if limit_rows else None

			# Handle cases where URL is required to disambiguate requested dataset
			ds_filter = src.filter(table_type, datasets.iloc[i]["Year"])
			url = datasets.iloc[i]['URL'] if len(ds_filter)>1 else None
			id = datasets.iloc[i]['dataset_id'] if len(ds_filter)>1 else None

			try:
				table = src.load(table_type, datasets.iloc[i]["Year"], pbar=True, nrows=nrows, 
					 url=url, id=id)
			except OPD_MinVersionError as e:
				e.prepend(f"Iteration {i}", srcName, table_type, datasets.iloc[i]["Year"])
				caught_exceptions_warn.append(e)
				continue
			except warn_errors as e:
				e.prepend(f"Iteration {i}", srcName, table_type, datasets.iloc[i]["Year"])
				update_outages(outages_file, datasets.iloc[i], True, e)
				caught_exceptions_warn.append(e)
				continue
			except (OPD_TooManyRequestsError, OPD_arcgisAuthInfoError) as e:
				# Catch exceptions related to URLs not functioning
				e.prepend(f"Iteration {i}", srcName, table_type, datasets.iloc[i]["Year"])
				update_outages(outages_file, datasets.iloc[i], True, e)
				caught_exceptions.append(e)
				continue
			except:
				raise

			if len(table.table)==0: 
				update_outages(outages_file, datasets.iloc[i], True, ValueError('Table has 0 rows'))
				continue

			update_outages(outages_file, datasets.iloc[i], False)

			if pd.notnull(datasets.iloc[i]['query']):
				for k,v in data_loaders.data_loader.str2json(datasets.iloc[i]['query']).items():
					assert (table.table[k]==v).all()


			if 'data-openjustice.doj.ca.gov' in datasets.iloc[i]['URL'].lower() and \
				'ripa' in datasets.iloc[i]['URL'].lower():
				assert (datasets.iloc[i]['Agency']==opd.defs.MULTI) + (len(table.table['AGENCY_NAME'].unique())==1)==1

			if isinstance(datasets.iloc[i]['dataset_id'], str) and ';' in datasets.iloc[i]['dataset_id']:
				# A year-long dataset is generated from multiple datasets. Check that whole year is covered
				# Parse dataset ID to get number of datasets IDs
				sheets = None
				dataset_list = datasets.iloc[i]['dataset_id']
				if '|' in dataset_list:  # dataset names are separated from relative URLs by |
					dataset_list = dataset_list.split('|')
					assert len(dataset_list)==2
					sheets = dataset_list[0].split(';')   # Different sheet names for each dataset are separated by ;. If multiple sheets for a given dataset, separate by &
					dataset_list = dataset_list[1]
				dataset_list = dataset_list.split(';')  
				
				table.standardize()

				assert opd.defs.columns.DATE in table.table
				months = table.table[opd.defs.columns.DATE].dt.month.unique()
				if len(months)!=12:
					missing_months = [x for x in range(1,13) if x not in months]
					if srcName=='Wallkill' and datasets.iloc[i]["Year"]==2016:
						# Only has last 3 quarters of data
						missing_months = [x for x in missing_months if x not in [1,2,3]]
					elif state=='California' and datasets.iloc[i]["Year"]==2018:
						# Only has last 3 quarters of data
						missing_months = [x for x in missing_months if x not in range(1,7)]
					elif srcName=='Albemarle County' and datasets.iloc[i]["Year"]==2021:
						# August and Sept data does not have date field
						missing_months = [x for x in missing_months if x not in [8,9]]
					for m in missing_months:
						url = datasets.iloc[i]["URL"] + '/' + dataset_list[m-1].strip()
						cur_sheet = sheets[min(m-1, len(sheets)-1)] if sheets else None
						loader = data_loaders.Excel(url, cur_sheet, datasets.iloc[i]["date_field"])
						df_check = loader.load()
						assert df_check['DATE'].apply(lambda x: (int(x[:2]) if isinstance(x,str) else x.month) !=m).all()

			if not pd.isnull(datasets.iloc[i]["date_field"]):
				if datasets.iloc[i]["date_field"] not in table.table or table.table[datasets.iloc[i]["date_field"]].isnull().all():
					table = src.load(table_type, datasets.iloc[i]["Year"], pbar=False, nrows=2000,
					 	url=url, id=id)
				assert datasets.iloc[i]["date_field"] in table.table
				#assuming a Pandas string dtype('O').name = object is okay too
				assert any([x in table.table[datasets.iloc[i]["date_field"]].dtype.name for x in ['datetime','period']]) or \
						table.table[datasets.iloc[i]["date_field"]].apply(lambda x: type(x) in [pd.Timestamp,pd.Period]).mean()>0.9
				dts = table.table[datasets.iloc[i]["date_field"]]
				dts = dts[dts.notnull()]
				# New Orleans complaints dataset has many empty dates
				# "Seattle and Minneapolis starts with bad date data"
				if len(dts)>0 or srcName not in ["Seattle","New Orleans",'Minneapolis','St. Paul','Virginia Beach','Tucson'] or \
					table_type not in [TableType.COMPLAINTS, TableType.INCIDENTS, TableType.CALLS_FOR_SERVICE]:
					assert len(dts) > 0   # If not, either all dates are bad or number of rows requested needs increased
					assert dts.iloc[0].year <= datetime.now().year if isinstance(dts.iloc[0], (pd.Timestamp,pd.Period)) else \
						dts.iloc[1].year <= datetime.now().year
			if not pd.isnull(datasets.iloc[i]["agency_field"]):
				assert datasets.iloc[i]["agency_field"] in table.table or "RAW_"+datasets.iloc[i]["agency_field"] in table.table

			# Adding a pause here to prevent issues with requesting from site too frequently
			if last_source!=srcName:
				last_source = srcName
				sleep_time = base_sleep_time
			else:
				sleep(sleep_time)
				sleep_time+=base_sleep_time

	if len(caught_exceptions)==1:
		raise caught_exceptions[0]
	elif len(caught_exceptions)>0:
		msg = f"{len(caught_exceptions)} URL errors encountered:\n"
		for e in caught_exceptions:
			msg += "\t" + e.args[0] + "\n"
		raise OPD_MultipleErrors(msg)

	for e in caught_exceptions_warn:
		warnings.warn(str(e))


def test_get_years_to_check():
	assert data._get_years_to_check([2020], cur_year=2023, force=True, isfile=False) == []
	assert data._get_years_to_check([2022], cur_year=2023, force=False, isfile=True) == []
	assert data._get_years_to_check([2022, 2020], cur_year=2023, force=False, isfile=False) == [2023]
	assert data._get_years_to_check([2020, 2021], cur_year=2023, force=True, isfile=True) == [2022, 2023]
	assert data._get_years_to_check([2020, 2021], cur_year=2023, force=True, isfile=False) == [2022, 2023]


@pytest.mark.parametrize('source,year,table_type,nrows,agency', [
	("Virginia",2020,"STOPS", 2000, "Fairfax County Police Department"), # CKAN
	("Philadelphia", 2021, "STOPS", 1000, None),  # Carto
	("Richmond","MULTIPLE","OFFICER-INVOLVED SHOOTINGS", 5, None), # Socrata
	("Fairfax County",2016,"ARRESTS", 1000, None),  # ArcGIS
	("Norristown", 2016, "USE OF FORCE",100, None), # Excel
	("Denver", "MULTIPLE", "OFFICER-INVOLVED SHOOTINGS", 50, None) # CSV
])
def test_load_gen(source, year, table_type, nrows, agency):
	if check_for_dataset(source, table_type):
		src = data.Source(source)
		max_iter = 10
		df = src.load(table_type, year, agency=agency, nrows=max_iter*nrows).table
		with warnings.catch_warnings():
			warnings.filterwarnings("ignore",category=RuntimeWarning)
			df = df.convert_dtypes()

		offset = 0
		k = 0
		for t in src.load_iter(table_type, year, nbatch=nrows, force=True, agency=agency):
			df_cur = df.iloc[offset:offset+len(t.table)].reset_index(drop=True)
			df2 = t.table.copy()
			if set(df.columns)!=set(df2.columns):
				# Expecting that a column was not returned because it is empty in the subset requested in this iteration
				assert len(df2.columns)<len(df.columns)
				missing_columns = list(set(df.columns).difference(set(df2.columns)))
				for col in missing_columns:
					assert df_cur[col].isnull().all()
					df_cur = df_cur.drop(columns=col)
			# Assure columns are in proper order
			assert set(df_cur.columns)==set(df2.columns)
			df2 = df2[df_cur.columns]
			# Ensure that dtypes match
			df2 = df2.astype(df_cur.dtypes.to_dict())
			assert df2.equals(df_cur)
			offset+=len(t.table)
			k+=1
			if k>=max_iter:
				break


def can_be_limited(data_type, url):
	if url.lower().endswith(".zip"):
		return False
	elif data_type in [DataType.ArcGIS, DataType.SOCRATA, DataType.CSV, DataType.EXCEL, DataType.CARTO, DataType.CKAN, DataType.HTML, DataType.OPENDATASOFT]:
		return True
	else:
		raise ValueError("Unknown table type")
	

if __name__ == "__main__":
	from test_utils import get_datasets
	# For testing
	use_changed_rows = False
	csvfile = None
	csvfile = os.path.join(r"..",'opd-data','opd_source_table.csv')
	start_idx = 0
	skip = None
	# skip = "Sacramento"
	source = None
	# source = "Mesa"
	query = {}
	# query = {'TableType':'TRAFFIC STOPS'}

	datasets = get_datasets(csvfile, use_changed_rows)

	# test_excel_sheets(csvfile, source, last, skip, None) 
	# test_get_years_to_check(csvfile, source, last, skip, None) 
	# test_table_type_warning(csvfile, source, last, skip, None) 
	# test_offsets_and_nrows(csvfile, source, last, skip, None) 
	# test_check_version(csvfile, None, last, skip, None) #
	test_source_download_limitable(datasets, source, start_idx, skip, False, query) 
	
	# test_get_count(csvfile, None, last, skip, None)
	# test_load_gen(csvfile, source, last, skip, None) 
	
