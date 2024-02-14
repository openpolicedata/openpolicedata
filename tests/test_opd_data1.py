import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, datasets, data_loaders
from openpolicedata.defs import DataType, TableType
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
from datetime import datetime
import pandas as pd
from time import sleep
import warnings
import os

sleep_time = 0.1

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

def get_datasets(csvfile):
	if csvfile is not None and not os.path.exists(csvfile):
		raise ValueError(f"Unable to find {csvfile}")

	if csvfile != None:
		datasets.datasets = datasets._build(csvfile)

	return datasets.datasets

def user_request_skip(datasets, i, skip, last, source):
	# Skip sources that the user requested to skip
	if skip != None:
		skip = skip.split(",")
		skip = [x.strip() for x in skip]
	if skip != None and datasets.iloc[i]["SourceName"] in skip:
		return True
	# User requested only the last values to run
	if last!=None and i < len(datasets) - last:
		return True
	if source != None and datasets.iloc[i]["SourceName"] != source:
		return True
	
	return False

class TestData:
	def check_table_type_warning(self, csvfile, source, last, skip, loghtml):
		sources = datasets.query().copy().iloc[0]
		sources["TableType"] = "TEST"
		with pytest.warns(UserWarning):
			data.Table(sources)

	def test_check_version(self, csvfile, source, last, skip, loghtml):
		ds = get_datasets(csvfile).iloc[0].copy()
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


	def test_offsets_and_nrows(self, csvfile, source, last, skip, loghtml):
		get_datasets(csvfile)
		src = data.Source("Philadelphia")
		df = src.load(year=2019, table_type="Officer-Involved Shootings").table
		offset = 1
		nrows = len(df)-2
		df_offset = src.load(year=2019, table_type="Officer-Involved Shootings", offset=offset, nrows=nrows).table
		assert df_offset.equals(df.iloc[offset:offset+nrows].reset_index(drop=True))

	def check_excel_sheets(self, csvfile, source, last, skip, loghtml):
		datasets = get_datasets(csvfile)
		for i in range(len(datasets)):
			if user_request_skip(datasets, i, skip, last, source):
				continue

			if datasets.iloc[i]["DataType"]!=DataType.EXCEL:
				continue

			srcName = datasets.iloc[i]["SourceName"]
			state = datasets.iloc[i]["State"]
			src = data.Source(srcName, state=state)

			table_print = datasets.iloc[i]["TableType"]
			now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
			print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

			excel = src._Source__get_loader(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"], 
				   dataset_id=datasets.iloc[i]["dataset_id"])
			sheets, has_year_sheets = excel._Excel__get_sheets()

			if has_year_sheets:
				# Ensure that load works
				src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], pbar=False)
			else:
				excel._Excel__check_sheet(sheets)

		
	def test_source_download_limitable(self, csvfile, source, last, skip, loghtml, query={}):
		datasets = get_datasets(csvfile)
		num_stanford = 0
		max_num_stanford = 1  # This data is standardized. Probably no need to test more than 1
		caught_exceptions = []
		caught_exceptions_warn = []
			
		for i in range(len(datasets)):
			if user_request_skip(datasets, i, skip, last, source):
				continue

			has_date_field = not pd.isnull(datasets.iloc[i]["date_field"])
			if can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]) or has_date_field:
				if is_stanford(datasets.iloc[i]["URL"]):
					num_stanford += 1
					if num_stanford > max_num_stanford:
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

				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

				try:
					table = src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], pbar=False, nrows=20)
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

				if len(table.table)==0 and has_outages and \
					(outages[["State","SourceName","Agency","TableType","Year"]] == datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]]).all(axis=1).any():
					caught_exceptions_warn.append(f'Outage continues for {str(datasets.iloc[i][["State","SourceName","Agency","TableType","Year"]])}')
					continue
				else:
					assert len(table.table)>0

				if not pd.isnull(datasets.iloc[i]["date_field"]):
					if datasets.iloc[i]["date_field"] not in table.table:
						table = src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], pbar=False, nrows=2000)
					assert datasets.iloc[i]["date_field"] in table.table
					#assuming a Pandas string dtype('O').name = object is okay too
					assert (table.table[datasets.iloc[i]["date_field"]].dtype.name in ['datetime64[ns]', 'datetime64[ns, UTC]', 
																		'datetime64[ms]','period[A-DEC]','period[Y-DEC]','period[Q-DEC]',
																		'period[M]']) or \
						    table.table[datasets.iloc[i]["date_field"]].apply(lambda x: type(x) in [pd.Timestamp,pd.Period]).all()
					dts = table.table[datasets.iloc[i]["date_field"]]
					dts = dts[dts.notnull()]
					# New Orleans complaints dataset has many empty dates
					# "Seattle and Minneapolis starts with bad date data"
					if len(dts)>0 or srcName not in ["Seattle","New Orleans",'Minneapolis'] or \
						datasets.iloc[i]["TableType"] not in [TableType.COMPLAINTS, TableType.INCIDENTS]:
						assert len(dts) > 0   # If not, either all dates are bad or number of rows requested needs increased
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

	def test_get_count(self, csvfile, source, last, skip, loghtml):
		get_datasets(csvfile)

		print("Testing Socrata source")
		src = data.Source("Richmond")
		loader = data_loaders.Socrata(src.datasets.iloc[0]["URL"], src.datasets.iloc[0]["dataset_id"], date_field=src.datasets.iloc[0]["date_field"])  
		year = 2021
		assert loader.get_count(year=year) == src.get_count(year=year, table_type=src.datasets.iloc[0]["TableType"])
		year = [2020,2022]
		assert loader.get_count(year=year) == src.get_count(year=year, table_type=src.datasets.iloc[0]["TableType"])

		print("Testing CKAN source")
		src = data.Source("Virginia")
		loader = data_loaders.Ckan(src.datasets.iloc[0]["URL"], src.datasets.iloc[0]["dataset_id"], date_field=src.datasets.iloc[0]["date_field"])  
		year = 2021
		assert loader.get_count(year=year) == src.get_count(year=year)
		year = [2020,2022]
		assert loader.get_count(year=year) == src.get_count(year=year)

		agency = "Arlington County Police Department"
		opt_filter = '"' + src.datasets.iloc[0]["agency_field"] + '"' + " = '" + agency + "'"
		year = 2021
		assert src.get_count(year=year, agency=agency) == loader.get_count(year=year, opt_filter=opt_filter)

		print("Testing ArcGIS source")
		src = data.Source("Charlotte-Mecklenburg")
		count = src.get_count(year="NONE", table_type="EMPLOYEE")

		url = "https://gis.charlottenc.gov/arcgis/rest/services/CMPD/CMPD/MapServer/16/"
		gis = data_loaders.Arcgis(url)
		assert count == gis.get_count()

		print("Testing CSV source")
		src = data.Source("Denver")

		url = "https://www.denvergov.org/media/gis/DataCatalog/denver_police_officer_involved_shootings/csv/denver_police_officer_involved_shootings.csv"
		loader = data_loaders.Csv(url, date_field="INCIDENT_DATE")

		count = src.get_count(table_type="OFFICER-INVOLVED SHOOTINGS", force=True)
			
		assert loader.get_count() == count

		print("Testing Excel source")
		src = data.Source("Rutland")

		url = "http://www.rutlandcitypolice.com/app/download/5136813/ResponseToResistance+2015-2017.xls"
		loader = data_loaders.Excel(url, date_field="Date")

		count = src.get_count(table_type="USE OF FORCE", force=True)
			
		assert loader.get_count(force=True) == count

		print("Testing Carto source")
		src = data.Source("Philadelphia")
		url = "phl"
		dataset = "car_ped_stops"
		date_field = "datetimeoccur"
		loader = data_loaders.Carto(url, dataset, date_field)  
		year = 2021
		assert loader.get_count(year=year) == src.get_count(year=year, table_type=TableType.STOPS)
		year = [2020,2022]
		assert loader.get_count(year=year) == src.get_count(year=year, table_type=TableType.STOPS)

	
	def test_get_years_to_check(self, csvfile, source, last, skip, loghtml):
		assert data._get_years_to_check([2020], cur_year=2023, force=True, isfile=False) == []
		assert data._get_years_to_check([2022], cur_year=2023, force=False, isfile=True) == []
		assert data._get_years_to_check([2022, 2020], cur_year=2023, force=False, isfile=False) == [2023]
		assert data._get_years_to_check([2020, 2021], cur_year=2023, force=True, isfile=True) == [2022, 2023]
		assert data._get_years_to_check([2020, 2021], cur_year=2023, force=True, isfile=False) == [2022, 2023]
	
	
	def test_load_gen(self, csvfile, source, last, skip, loghtml):
		datasets = [
			("Virginia",2020,"STOPS", 2000, "Fairfax County Police Department"), # CKAN
			("Philadelphia", 2021, "STOPS", 1000),  # Carto
			("Richmond","MULTIPLE","OFFICER-INVOLVED SHOOTINGS", 5), # Socrata
			("Fairfax County",2016,"ARRESTS", 1000),  # ArcGIS
			("Norristown", 2016, "USE OF FORCE",100), # Excel
			("Denver", "MULTIPLE", "OFFICER-INVOLVED SHOOTINGS", 50) # CSV
			] 

		for ds in datasets:
			src = data.Source(ds[0])
			agency = ds[4] if len(ds)>4 else None
			max_iter = 10
			df = src.load(ds[2], ds[1], agency=agency, nrows=max_iter*ds[3]).table
			with warnings.catch_warnings():
				warnings.filterwarnings("ignore",category=RuntimeWarning)
				df = df.convert_dtypes()

			offset = 0
			k = 0
			for t in src.load_iter(ds[2], ds[1], nbatch=ds[3], force=True, agency=agency):
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
	if (data_type == DataType.CSV and ".zip" in url):
		return False
	elif data_type in [DataType.ArcGIS, DataType.SOCRATA, DataType.CSV, DataType.EXCEL, DataType.CARTO, DataType.CKAN]:
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
	# csvfile = r"..\opd-data\opd_source_table.csv"
	last = None
	# last = 922-634+1
	skip = None
	# skip = "Greensboro"
	source = None
	# source = "Tucson"

	# tp.check_excel_sheets(csvfile, source, last, skip, None) 
	# tp.test_get_years_to_check(csvfile, source, last, skip, None) 
	# tp.check_table_type_warning(csvfile, source, last, skip, None) 
	# tp.test_offsets_and_nrows(csvfile, source, last, skip, None) 
	# tp.test_check_version(csvfile, None, last, skip, None) #
	tp.test_source_download_limitable(csvfile, source, last, skip, None, query={'DataType':'CKAN'}) 
	
	# tp.test_get_count(csvfile, None, last, skip, None)
	# tp.test_load_gen(csvfile, source, last, skip, None) 
	
