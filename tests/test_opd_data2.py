if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import datasets
from openpolicedata.defs import MULTI, TableType
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
        datasets.datasets = datasets._build(csvfile)

    return datasets.query()

class TestData:
	def test_source_download_limitable(self, csvfile, source, last, skip, loghtml):
		if last == None:
			last = float('inf')
		datasets = get_datasets(csvfile)
		num_stanford = 0
		max_num_stanford = 1  # This data is standardized. Probably no need to test more than 1
		caught_exceptions = []
		caught_exceptions_warn = []
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
			has_date_field = not pd.isnull(datasets.iloc[i]["date_field"])
			if can_be_limited(datasets.iloc[i]["DataType"], datasets.iloc[i]["URL"]) or has_date_field:
				if is_stanford(datasets.iloc[i]["URL"]):
					num_stanford += 1
					if num_stanford > max_num_stanford:
						continue
				srcName = datasets.iloc[i]["SourceName"]
				state = datasets.iloc[i]["State"]
				src = data.Source(srcName, state=state)
				# For speed, set private limit parameter so that only a single entry is requested
				src._Source__limit = 20

				table_print = datasets.iloc[i]["TableType"]
				now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
				print(f"{now} Testing {i} of {len(datasets)}: {srcName} {table_print} table")

				try:
					table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"], pbar=False)
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

				assert len(table.table)>0
				if not pd.isnull(datasets.iloc[i]["date_field"]):
					assert datasets.iloc[i]["date_field"] in table.table
					#assuming a Pandas string dtype('O').name = object is okay too
					assert (table.table[datasets.iloc[i]["date_field"]].dtype.name in ['datetime64[ns]', 'datetime64[ms]'])
					dts = table.table[datasets.iloc[i]["date_field"]]
					dts = dts[dts.notnull()]
					# New Orleans complaints dataset has many empty dates
					# "Seattle starts with bad date data"
					if len(dts)>0 or srcName not in ["Seattle","New Orleans"] or datasets.iloc[i]["TableType"]!=TableType.COMPLAINTS.value:
						assert len(dts) > 0   # If not, either all dates are bad or number of rows requested needs increased
						# Check that year is reasonable
						assert dts.iloc[0].year >= 1909  # This is just an arbitrarily old year that is assumed to be before all available data
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
				print(f"{now} Testing {i} of {len(datasets)}: {srcName} {table_print} table")

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
		src._Source__limit = 100
		table = src.load_from_url(2021, agency=agency, pbar=False)
		
		assert len(table.table)==100
		assert table.table[table._agency_field].nunique()==1
		assert table.table.iloc[0][table._agency_field] == agency

	def test_to_csv(self, csvfile, source, last, skip, loghtml):
		src = data.Source("Virginia")
		get_datasets(csvfile)
		agency="Fairfax County Police Department"
		src._Source__limit = 100
		year = 2021
		table = src.load_from_url(2021, agency=agency, pbar=False)

		table.to_csv()

		filename = table.get_csv_filename()
		assert os.path.exists(filename)

		# Load table back in
		src.load_from_csv(year, agency=agency)

		os.remove(filename)

	
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
	tp.test_source_download_limitable(r"..\opd-data\opd_source_table.csv", None, 1, None, None) 
