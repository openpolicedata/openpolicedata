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

sleep_time = 0.1
log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

# Set Arcgis data loader to validate queries with arcgis package if installed
opd.data_loaders._verify_arcgis = True

warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError)

def get_datasets(csvfile):
    if csvfile != None:
        datasets.datasets = datasets._build(csvfile)

    return datasets.query()

class TestData:
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
			if is_filterable(datasets.iloc[i]["DataType"]) or datasets.iloc[i]["Year"] != MULTI or \
				datasets.iloc[i]["DataType"] == DataType.EXCEL.value:  # If Excel, we can possibly check
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)

				if datasets.iloc[i]["DataType"] == DataType.EXCEL.value:
					loader = opd.data_loaders.Excel(datasets.iloc[i]["URL"])
					has_year_sheets = loader._Excel__get_sheets()[1]
					if not has_year_sheets:
						continue


				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

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
				print(f"{now} Testing {i+1} of {len(datasets)}: {srcName} {table_print} table")

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
		table = src.load_from_url(2021, agency=agency, pbar=False, nrows=100)
		
		assert len(table.table)==100
		assert table.table[table._agency_field].nunique()==1
		assert table.table.iloc[0][table._agency_field] == agency

	def test_to_csv(self, csvfile, source, last, skip, loghtml):
		src = data.Source("Virginia")
		get_datasets(csvfile)
		agency="Fairfax County Police Department"
		year = 2021
		table = src.load_from_url(2021, agency=agency, pbar=False, nrows=100)

		table.to_csv()

		filename = table.get_csv_filename()
		assert os.path.exists(filename)

		# Load table back in
		src.load_from_csv(year, agency=agency)

		os.remove(filename)

	
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
	csvfile = os.path.join("..","opd-data","opd_source_table.csv")
	last = None
	last = 863-791+1
	source = None
	# source = "Washington D.C."
	skip = None
	# skip = "Fayetteville,Seattle"
	tp.test_get_years(csvfile, source, last, skip, None)
	# tp.test_get_agencies(csvfile, None, None, skip, None)
	# tp.test_get_agencies_name_match(csvfile, None, None, skip, None)
	# tp.test_agency_filter(csvfile, None, None, skip, None)
	# tp.test_to_csv(csvfile, None, None, skip, None)
