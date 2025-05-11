import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, data_loaders
from openpolicedata.defs import DataType, TableType
from openpolicedata import exceptions
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
import openpolicedata as opd
from datetime import datetime
from io import StringIO
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

@pytest.fixture()
def log_stream():
    stream = StringIO()
    yield stream
    stream.truncate(0)
    stream.seek(0)
    assert len(stream.getvalue()) == 0

@pytest.fixture()
def logger(log_stream):
    logger = opd.log.get_logger()
    # Redirect handler output so that it can be checked
    logger.handlers[0].setStream(log_stream)

    yield logger
    for handler in logger.handlers:
        if handler.name != opd.log.stream_handler_name:
            logger.removeHandler(handler)

def check_table_type_warning(all_datasets):
	sources = all_datasets.copy().iloc[0]
	sources["TableType"] = "TEST"
	with pytest.warns(UserWarning):
		data.Table(sources)

def test_not_verbose(logger, log_stream):
	source = 'Lansing'
	table = "OFFICER-INVOLVED SHOOTINGS"
	if check_for_dataset(source, table):
		src = opd.Source(source)
		table = src.load(table, 'MULTIPLE')
		assert len(log_stream.getvalue()) == 0

def test_verbose(logger, log_stream):
	source = 'Lansing'
	table = "OFFICER-INVOLVED SHOOTINGS"
	if check_for_dataset(source, table):
		src = opd.Source(source)
		table = src.load(table, 'MULTIPLE', verbose=True)
		assert len(log_stream.getvalue())>0

@pytest.mark.parametrize('source, table, year', [('Phoenix', "OFFICER-INVOLVED SHOOTINGS", 2022), 
				('Orlando', "OFFICER-INVOLVED SHOOTINGS", 2022), ('Indianapolis', "OFFICER-INVOLVED SHOOTINGS", 2022),
				('Philadelphia', "OFFICER-INVOLVED SHOOTINGS", 2018)])
def test_format_date_false(all_datasets, source, table, year):
	if check_for_dataset(source, table):
		src = opd.Source(source)
		table = src.load(table, year, format_date=False, nrows=1)
		# Confirm date has not been formatted
		assert isinstance(table.table[table.date_field].iloc[0],str)
		

@pytest.mark.parametrize('source, table, year', [('Denver', "OFFICER-INVOLVED SHOOTINGS", 2022), 
				('Sparks', "OFFICER-INVOLVED SHOOTINGS", 2022),  ('Louisville', "TRAFFIC STOPS", ['2018-12-29', '2019-01-01'])])
def test_format_date_false_not_allowed(all_datasets, source, table, year):
	if check_for_dataset(source, table):
		src = opd.Source(source)
		with pytest.raises(ValueError, match='Dates cannot be filtered'):
			src.load(table, year, format_date=False, nrows=1)


def test_check_version(datasets):
	ds = datasets.iloc[0].copy()
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

def test_check_python_version(datasets):
	ds = datasets.iloc[0].copy()
	# Set min_version to create error
	ds["py_min_version"] = "100000.0"
	with pytest.raises(OPD_MinVersionError):
		data._check_version(ds)

	# These should pass
	ds["py_min_version"] = "0.0"
	data._check_version(ds)
	ds["py_min_version"] = pd.NA
	data._check_version(ds)

def test_single_year_filter():
	if check_for_dataset('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Phoenix')
		year = 2017
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, year, None, None)
		assert not filter_by_year
		assert dataset['Year']==year
		assert dataset['TableType']==opd.defs.TableType.CALLS_FOR_SERVICE

def test_single_year_range_filter():
	if check_for_dataset('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Phoenix')
		with pytest.raises(ValueError, match="There are no sources matching"):
			src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, [2017,2018], None, None)

@pytest.mark.parametrize('year, exp_filt_by_year', [(2017,True), (opd.defs.MULTI,False)])
def test_multi_year_filter(year,exp_filt_by_year):
	if check_for_dataset('Norristown', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Norristown')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)
		assert filter_by_year == exp_filt_by_year
		assert dataset['Year']==opd.defs.MULTI
		assert dataset['TableType']==opd.defs.TableType.USE_OF_FORCE

def test_multi_year_range_filter_includes_single_year():
	if check_for_dataset('Norristown', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Norristown')
		year = [2016,2018]
		with pytest.raises(ValueError, match="Year range cannot contain the year corresponding to a single year"):
			src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)

def test_bad_year_range():
	if check_for_dataset('Norristown', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Norristown')
		year = [2016,2018, 2017]
		with pytest.raises(ValueError, match="year input must either be a single year or a list containing a start and stop year"):
			src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)

@pytest.mark.parametrize('src_name, state, agency', [('ERROR',None,None), ('ERROR','Arizona',None), ('Chandler','Arizona', 'ERROR'),
													 ('Chandler','ERROR', None)])
def test_no_source_match(all_datasets, src_name, state, agency):
	with pytest.raises(ValueError):
		data.Source(src_name, state, agency)

def test_single_agency(all_datasets):
	if check_for_dataset('Chandler', opd.defs.TableType.ARRESTS):
		data.Source('Chandler')

def test_multiple_agency_same_source_name_error(all_datasets):
	if check_for_dataset('Contra Costa County', opd.defs.TableType.STOPS):
		with pytest.raises(exceptions.MultiAgencySourceError):
			data.Source('Contra Costa County')

@pytest.mark.parametrize('agency', ['MULTIPLE', 'Contra Costa County'])
def test_multiple_agency_same_source_name(all_datasets, agency):
	if check_for_dataset('Contra Costa County', opd.defs.TableType.STOPS):
		data.Source('Contra Costa County', agency=agency)

@pytest.mark.parametrize('year, url', [(2019, "APDUseOfForce/FeatureServer/0"), 
												([2018,2019], "APDUseOfForce/FeatureServer/0"), (2021, "APD_UseOfForce2021/FeatureServer/0")])
def test_multiple_multiple_year_filter(all_datasets, year, url):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)
		assert filter_by_year
		assert dataset['Year']==opd.defs.MULTI
		assert dataset['TableType']==opd.defs.TableType.USE_OF_FORCE
		assert url in dataset['URL']

@pytest.mark.parametrize('year, url, url_test', [(2020, None, 'LMPD_STOPS_DATA_(2)'), 
												(2021, None, 'LMPD_STOP_DATA_2021'),
												(2021, 'LMPD_STOPS_DATA_(2)', 'LMPD_STOPS_DATA_(2)')])
def test_overlapping_multi_and_single(all_datasets, year, url, url_test):
	if check_for_dataset('Louisville', opd.defs.TableType.TRAFFIC):
		src = data.Source('Louisville')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.TRAFFIC, year, url, None)
		assert url_test in dataset['URL']

@pytest.mark.parametrize('year, id, id_test', [(2020, None, 'ex94-c5ad'), 
												(2021, None, 'izhu-764k'),
												([2020,2021], 'izhu-764k', 'izhu-764k')])
def test_overlapping_multi_and_single_with_id(all_datasets, year, id, id_test):
	if check_for_dataset('Mesa', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Mesa')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, year, None, id)
		assert id_test == dataset['dataset_id']

@pytest.mark.parametrize('year', [2020, [2020,2021], opd.defs.MULTI])
def test_multiple_bad_multiple_year_filter(all_datasets, year):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		with pytest.raises(ValueError, match="There is more than one source matching "):
			src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)

@pytest.mark.parametrize('year, exp_filt_by_year', [([2021,2022], True), (opd.defs.MULTI, False)])
def test_url_input(all_datasets, year, exp_filt_by_year):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		url = "APDUseOfForce/FeatureServer/0"
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, url, None)
		assert filter_by_year == exp_filt_by_year
		assert dataset['Year']==opd.defs.MULTI
		assert dataset['TableType']==opd.defs.TableType.USE_OF_FORCE
		assert url in dataset['URL']


def test_offsets_and_nrows():
	if check_for_dataset('Philadelphia', opd.defs.TableType.SHOOTINGS):
		src = data.Source("Philadelphia")
		df = src.load(year=2019, table_type="Officer-Involved Shootings").table
		offset = 1
		nrows = len(df)-2
		df_offset = src.load(year=2019, table_type="Officer-Involved Shootings", offset=offset, nrows=nrows).table
		assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

def check_excel_sheets(datasets, source, start_idx, skip):
	for i in range(len(datasets)):
		if user_request_skip(datasets, i, skip, start_idx, source):
			continue

		if datasets.iloc[i]["DataType"]!=DataType.EXCEL:
			continue

		srcName = datasets.iloc[i]["SourceName"]
		state = datasets.iloc[i]["State"]
		src = data.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

		table_print = datasets.iloc[i]["TableType"]
		now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
		print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

		excel = src._Source__get_loader(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"], datasets.iloc[i]["query"],
				dataset_id=datasets.iloc[i]["dataset_id"])
		sheets, has_year_sheets = excel._Excel__get_sheets()

		if has_year_sheets:
			# Ensure that load works
			src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], pbar=False)
		else:
			excel._Excel__check_sheet(sheets)


def test_source_download_limitable(datasets, source, start_idx, skip, loghtml, query={}):
	caught_exceptions = []
	caught_exceptions_warn = []
	last_source = None
	for i in range(len(datasets)):
		if user_request_skip(datasets, i, skip, start_idx, source):
			continue

		if can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]):
			match = True
			for k,v in query.items():
				if datasets.iloc[i][k]!=v:
					match = False
					break
			if not match:
				continue

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
			ds_filter, _ = src._Source__filter_for_source(table_type, datasets.iloc[i]["Year"], None, None, errors=False)
			url = datasets.iloc[i]['URL'] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None
			id = datasets.iloc[i]['dataset_id'] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None

			try:
				table = src.load(table_type, datasets.iloc[i]["Year"], pbar=True, nrows=nrows, 
					 url=url, id=id)
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
			else:
				assert len(table.table)>0

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
				assert (table.table[datasets.iloc[i]["date_field"]].dtype.name in ['datetime64[ns]', 'datetime64[ns, UTC]', 
																	'datetime64[ms]', 'period[Y-DEC]','period[Q-DEC]',
																	'period[M]']) or \
						table.table[datasets.iloc[i]["date_field"]].apply(lambda x: type(x) in [pd.Timestamp,pd.Period]).mean()>0.9
				dts = table.table[datasets.iloc[i]["date_field"]]
				dts = dts[dts.notnull()]
				# New Orleans complaints dataset has many empty dates
				# "Seattle and Minneapolis starts with bad date data"
				if len(dts)>0 or srcName not in ["Seattle","New Orleans",'Minneapolis','St. Paul'] or \
					table_type not in [TableType.COMPLAINTS, TableType.INCIDENTS]:
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

@pytest.mark.parametrize("loader, source, table_type, agency, years", [
	(data_loaders.Socrata, 'Richmond', 'CALLS FOR SERVICE', None, [2021, [2020, 2022]]),
	(data_loaders.Ckan, 'Virginia', 'STOPS', "Arlington County Police Department", [2021, [2020, 2022]]),
	(data_loaders.Arcgis, "Charlotte-Mecklenburg", 'EMPLOYEE', None, []),
	(data_loaders.Csv, 'Denver', "OFFICER-INVOLVED SHOOTINGS", None, []),
	(data_loaders.Excel, 'Rutland', "USE OF FORCE", None, []),
	(data_loaders.Carto, "Philadelphia", 'STOPS', None, [2021, [2020, 2022]])
	])
def test_get_count(datasets, loader, source, table_type, agency, years):
	if check_for_dataset(source, table_type):
		print(f"Testing {loader} source")
		src = opd.Source(source)
		i = src.datasets["TableType"] == table_type
		i = i[i].index[0]
		if pd.notnull(src.datasets.loc[i]["dataset_id"]):
			loader = loader(src.datasets.loc[i]["URL"], data_set=src.datasets.loc[i]["dataset_id"], date_field=src.datasets.loc[i]["date_field"])
		else:
			loader = loader(src.datasets.loc[i]["URL"], date_field=src.datasets.loc[i]["date_field"])
		if len(years)==0:
			assert loader.get_count(force=True) == src.get_count(year=src.datasets.loc[i]['Year'], table_type=table_type, force=True)
		else: 
			for year in years:
				assert loader.get_count(year=year, force=True) == src.get_count(year=year, table_type=table_type, force=True)

		if agency:
			opt_filter = '"' + src.datasets.loc[i]["agency_field"] + '"' + " = '" + agency + "'"
			year = years[0]
			assert src.get_count(year=year, agency=agency, force=True) == loader.get_count(year=year, opt_filter=opt_filter, force=True)


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
					df_cur.drop(columns=col, inplace=True)
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
	elif data_type in [DataType.ArcGIS, DataType.SOCRATA, DataType.CSV, DataType.EXCEL, DataType.CARTO, DataType.CKAN, DataType.HTML]:
		return True
	else:
		raise ValueError("Unknown table type")
	

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
	csvfile = os.path.join(r"..",'opd-data','opd_source_table.csv')
	start_idx = 0
	skip = None
	# skip = "Sacramento"
	source = None
	# source = "St. Paul" #"Washington D.C." #"Wallkill"
	query = {}
	# query = {'SourceName':'San Francisco'}

	datasets = get_datasets(csvfile, use_changed_rows)

	# check_excel_sheets(csvfile, source, last, skip, None) 
	# test_get_years_to_check(csvfile, source, last, skip, None) 
	# check_table_type_warning(csvfile, source, last, skip, None) 
	# test_offsets_and_nrows(csvfile, source, last, skip, None) 
	# test_check_version(csvfile, None, last, skip, None) #
	test_source_download_limitable(datasets, source, start_idx, skip, False, query) 
	
	# test_get_count(csvfile, None, last, skip, None)
	# test_load_gen(csvfile, source, last, skip, None) 
	
