from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from datetime import datetime
from io import BytesIO
import numbers
import json
import pandas as pd
from math import ceil
import requests
from time import sleep
from tqdm import tqdm
import urllib
import urllib3
import warnings
from zipfile import ZipFile

from ..datetime_parser import to_datetime
from .. import log, httpio
from .. import defs

try:
	import geopandas as gpd
	_has_gpd = True
except:
	_has_gpd = False

logger = log.get_logger()

# Global parameter for testing both with and without GeoPandas in testing
_use_gpd_force = None

# Default number of records to read per request
_default_limit = 100000

sleep_time = 0.1

_url_error_msg = "There is likely an issue with the website. Open the URL {} with a web browser to confirm. " + \
					"See a list of known site outages at https://github.com/openpolicedata/opd-data/blob/main/outages.csv"


def _filter_inaccurate_date_query(df, date_field, date, format_date, offset, nrows):
	if not format_date:
		raise ValueError("Dates cannot be filtered if format_date is False for this dataset due to the date column not being a "+
							"date data type at the source. Note: most other datasets will work fine if format_date is False")
	logger.debug(f"User requested filtering by a date range but this was NOT done in the query "+
					f"due to the date field not being in a date format. Converting {date_field} column to "
					f"a datetime in order to filter for requested date range {date}")
	
	try:
		df[date_field] = to_datetime(df[date_field])
	except:
		df[date_field] = to_datetime(df[date_field], errors='coerce')

	if isinstance(df[date_field].dtype, pd.PeriodDtype):
		raise ValueError('Periods cannot be filtered by date, only year.')  # This code should never be reached
	else:
		df = df[ (df[date_field] >= date[0]) & (df[date_field] < date[1]+pd.Timedelta('1D')) ]

	if offset!=None and offset>0:
		df = df.iloc[offset:]
	if nrows!=None:
		df = df.head(nrows)
	df = df.reset_index(drop=True)
	return df


def _clean_date_input(date):
	if date==None or (isinstance(date, str) and date in [defs.MULTI, defs.NA]):
		return date
	
	date = [date, date] if not isinstance(date, list) else date
	
	# Make copy so original isn't changed
	date = date.copy() if isinstance(date, list) else date

	if len(date)!=2:
		raise ValueError("List of start/stop dates is not length 2")
	
	for k in range(2):
		if isinstance(date[k], str) and date[k].isdigit():
			date[k] = int(date[k])

		if isinstance(date[k], numbers.Number):
			if 999 < date[k] < 10000 and date[k]==round(date[k]):
				# Assume this is a year
				date[k] = f'{date[k]}-01-01' if k==0 else f'{date[k]}-12-31'
			else:
				raise ValueError(f"Unable to parse number {date[k]} as a year")

		date[k] = pd.to_datetime(date[k])
		dt_new = date[k].floor('24h')  # Removed time. Times are currently ignored.
		if date[k]!=dt_new:
			warnings.warn(f"Times in date filter requests are ignored. Changing {date[k]} to {dt_new}")
			date[k] = dt_new

	if date[0] > date[1]:
		raise ValueError(f'Start date must be less than stop date. Invalid input: {date}')
	
	return date


def _process_date(date, datetime_format=None, is_date_string=False):
	date = [x.strftime('%Y-%m-%d') for x in date]

	start_date = date[0]
	stop_date  = str(date[1])+'zz' if is_date_string else str(date[1])+"T23:59:59.999"

	if datetime_format:
		start_date = datetime.strftime(pd.to_datetime(start_date), datetime_format)
		stop_date = datetime.strftime(pd.to_datetime(stop_date), datetime_format)

	if start_date > stop_date:
		raise ValueError(f'Start date {start_date} needs to be less than or equal to stop date {stop_date}')

	return start_date, stop_date


def _filter_dataframe(df, date_field=None, date_filter=None, agency_field=None, agency=None, format_date=True):
	'''Filter dataframe by agency and/or date range
	
	Parameters
	----------
	df : pandas or geopandas dataframe
		Dataframe containing the data
	date_field : str
		(Optional) Name of the column that contains the date
	date_filter : int or a length 2 list of start and stop year(s), date string(s), and/or timestamp(s)
			(Optional) Define timespan of data to request count for:
				1. Request data for an entire year by inputting the year (i.e. 2023)
				2. Request data from a start year or datetime to a stop year or datetime using a length 2 list (i.e. [2021, '2023-02-01'] for start of 2021 to end of 2023-02-01)
	agency_field : str
		(Optional) Name of the column that contains the agency name (i.e. name of the police departments)
	agency : str
		(Optional) Name of the agency to filter for. None value returns data for all agencies.
	format_date : bool, optional
		If True, known date columns (based on presence of date_field in datasets table or data type information provided by dataset owner) will be automatically formatted
		to be pandas datetimes (or pandas Period in rare cases), by default True
	'''

	date_filter = _clean_date_input(date_filter)

	if agency:
		if not agency_field:
			raise ValueError(f'Agency filtering requested but no agency field was provided')
		
		logger.debug(f"Keeping values of column {agency_field} that are equal to {agency}")
		df = df[df[agency_field]==agency]

	if pd.notnull(date_field):
		is_year = date_field.lower()=='year'
		if not is_year and pd.api.types.is_integer_dtype(df[date_field]):
			is_year = ((df[date_field] >= 1900) & (df[date_field] <= 2200)).all()

		if format_date and not is_year and not hasattr(df[date_field], "dt"):
			with warnings.catch_warnings():
				# Ignore future warning about how this operation will be attempted to be done inplace:
				# In a future version, `df.iloc[:, i] = newvals` will attempt to set the values inplace instead of always setting a new array. 
				# To retain the old behavior, use either `df[df.columns[i]] = newvals` or, if columns are non-unique, `df.isetitem(i, newvals)`
				logger.debug(f"Converting values in column {date_field} to datetime objects")
				try:
					df[date_field] = to_datetime(df[date_field], ignore_errors=True)
				except:
					if date_filter is not None:
						raise ValueError(f"Unable to convert column {date_field} to a datetime. Date filter cannot be applied.")
					return df
		
	if date_filter is not None:
		if pd.isnull(date_field):
			raise ValueError(f'Date filtering requested but no date field was provided')
		elif not format_date:
			raise ValueError("Dates cannot be filtered if format_date is False")
				
		if not is_year:
			for k in range(len(date_filter)):
				dt_new = pd.to_datetime(date_filter[k]).floor('24h')  # Removed time. Times are currently ignored.
				if date_filter[k]!=dt_new:
					warnings.warn(f"Times in date filter requests are ignored. Changing {date_filter[k]} to {dt_new}")
					date_filter[k] = dt_new

			df = df[(df[date_field] >= date_filter[0]) & (df[date_field] < date_filter[1]+pd.Timedelta('1D'))]
		elif date_filter[0].month==1 and date_filter[0].day==1 and \
			date_filter[1].month==12 and date_filter[1].day==31:  # Requested full years
			logger.debug(f"Column {date_field} has been identfied as a year column")
			logger.debug(f"Keeping values of column {date_field} between {date_filter[0]} and {date_filter[1]}")
			dates = df[date_field].apply(lambda x: int(x) if isinstance(x,str) and x.isdigit() else x)
			df = df[dates.isin(range(date_filter[0].year, date_filter[1].year+1))]
		else:
			raise ValueError(f"Column {date_field} has been identfied as a year column and cannot be filtered by dates: {date_filter}")

	return df

def get_legacy_session():
	try:
		import ssl
	except:
		raise ImportError(f"Loading this dataset requires the SSL package, which typically comes with the Python installation" + 
						  " but is not for some Python versions like the one used by Jupyter Lite. To install, run 'pip install ssl'")
	ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
	ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
	session = requests.session()
	session.mount('https://', CustomHttpAdapter(ctx))
	return session


@dataclass
class Where:
	where: str
	accurate: bool = True
	count: int = None

	def __lt__(self, y):
		return self.where < y.where


# Based on https://stackoverflow.com/a/73519818/9922439
class CustomHttpAdapter (requests.adapters.HTTPAdapter):
	# "Transport adapter" that allows us to use custom ssl_context.

	def __init__(self, ssl_context=None, **kwargs):
		self.ssl_context = ssl_context
		super().__init__(**kwargs)

	def init_poolmanager(self, connections, maxsize, block=False):
		self.poolmanager = urllib3.poolmanager.PoolManager(
			num_pools=connections, maxsize=maxsize,
			block=block, ssl_context=self.ssl_context)
		

class UrlIoContextManager:
	def __init__(self, url) -> None:
		self.url = url
		try:
			self.file = httpio.open(url)
			self.ishttp = True
		except httpio.HTTPIOError:
			open_url =  urllib.request.urlopen(url)
			self.file = BytesIO(open_url.read())
			self.ishttp = False

	def __enter__(self):
		return self.file
	
	def __exit__(self, exc_type, exc_value, exc_tb):
		if self.ishttp:
			self.file.close()
		

def download_zip_and_extract(url, block_size, pbar=True):
	r = requests.get(url, stream=True)
	r.raise_for_status()
	total_size = int(r.headers.get("Content-Length", 0))
	pbar = pbar and total_size > block_size
	if pbar:
		bar = tqdm(
			desc=f"Downloading zip file: {url}",
			total=total_size,
			unit="iB",
			unit_scale=True,
			unit_divisor=1024,
			leave=False
		)
	b = BytesIO()
	for data in r.iter_content(block_size):
		b.write(data)
		if pbar:
			bar.update(len(data))
	r.close()

	logger.debug(f'Completed downloading CSV zip file: {url}')
	if pbar:
		bar.close()
	b.seek(0)

	logger.debug('Creating zip file')
	with ZipFile(b, 'r') as z:
		if len(z.namelist())>1:
			raise ValueError(f"More than 1 file found in {url} but no file was specified by the user. Please specify 1 or more files in the dataset input.")

		logger.debug('Reading from zip file')
		zip_data = z.read(z.namelist()[0])
		b.close()
		logger.debug('Converting to BytesIO')

	return zip_data

def str2json(json_str):
	if pd.isnull(json_str):
		return {}
	elif isinstance(json_str, dict):
		return json_str
	# Remove any curly quotes
	json_str = json_str.replace('“','"').replace('”','"')
	return json.loads(json_str)


class Data_Loader(ABC):
	"""Base class for data loaders

	Methods
	-------
	load(date=None, nrows=None, pbar=True, agency=None, opt_filter=None, select=None, output_type=None)
		Load data for query
	get_count(date=None, agency=None, force=False, opt_filter=None, where=None)
		Get number of records/rows generated by query
	get_years(nrows=1)
		Get years contained in data set
	"""

	_last_count = None

	@abstractmethod
	def isfile(self):
		pass

	@abstractmethod
	def get_count(self, date=None, *, agency=None, force=False, opt_filter=None, where=None):
		pass

	@abstractmethod
	def load(self, date=None, nrows=None, offset=0, *, pbar=True, agency=None, opt_filter=None, select=None, output_type=None, format_date=True):
		pass

	def get_years(self, *, nrows=1, check=None, **kwargs):
		'''Get years contained in data set
		
		Parameters
		----------
		nrows : int
			(Optional) Number of records to load when checking each year
			
		Returns
		-------
		list
			list containing years in data set
		'''

		if self.date_field==None:
			raise ValueError("A date field is required to get years")

		check_input = check is not None

		if check_input and len(check)==0:
			return []

		if check_input:
			check = check.copy()
			check.sort(reverse=True)
			year = check.pop(0)
		else:
			year = datetime.date.today().year

		oldest_recent = 20
		max_misses_gap = 10
		max_misses = oldest_recent
		misses = 0
		years = []
		while misses < max_misses:
			count = self.get_count(date=year)

			if count==0:  # If doesn't have len attribute, it is None
				misses+=1
			else:
				misses = 0
				max_misses = max_misses_gap
				years.append(year)

			sleep(sleep_time)

			year-=1
			if check_input:
				if len(check)==0:
					break
				while year not in check:
					year-=1
				check.remove(year)

		return years
 

def _setup_records_request(where, nrows, offset, sortby, date_field):
	nrows_after_read = None
	offset_after_read = None

	where = [w for w in where if w.count>0]
	record_counts = [w.count for w in where]

	if len(where)==0:
		nrows_req = record_counts
	elif len(where)>1:
		if nrows==None and offset==0:
			nrows_req = record_counts
		elif sortby in ['date', date_field] or any(not w.accurate for w in where):
			# Data will be read in with multiple where commands. Sorting by date can only be done after the fact
			nrows_after_read = nrows
			offset_after_read = offset
			offset = 0
			nrows_req = record_counts
		else:
			for k in range(len(where)):
				if offset<record_counts[k]:
					record_counts[k] -= offset  # Max records that can be requested starting from offset
					break
				else:
					offset-=record_counts[k]
					record_counts[k]=0

			where = [w for w,c in zip(where, record_counts) if c>0]
			record_counts = [c for c in record_counts if c>0]

			if len(where)==0:
				return pd.DataFrame()
			
			nrows_req = record_counts
			if nrows!=None and nrows < sum(record_counts):
				count = 0
				for k in range(len(record_counts)):
					if count >= nrows:
						where = where[:k]
						nrows_req = nrows_req[:k]
						record_counts = record_counts[:k]
						break
					else:
						nrows_req[k] = min(record_counts[k], nrows-count)
						count+=nrows_req[k]
	else:
		count = where[0].count - offset  # Max records that can be requested starting from offset
		if count<=0:
			return [], [], None, None, None
		
		nrows_req = [nrows]
		accurate_count = all(w.accurate for w in where)
		if nrows==None or nrows > count or not accurate_count:
			if not accurate_count:
				nrows_after_read = nrows
				offset_after_read = 0
			nrows_req = [count]

	return where, nrows_req, nrows_after_read, offset_after_read, offset


def _split_batches(nrows_req):
	batch_sizes = [x if x < _default_limit else _default_limit for x in nrows_req]
	num_batches = [ceil(x / y) for x,y in zip(nrows_req, batch_sizes)]

	return batch_sizes, num_batches


def _update_last_count(_last_count, date, where, opt_filter=None):
	if all(w.accurate for w in where):
		only_where = [w.where for w in where]
		return ((date, opt_filter, only_where), where) 
	else:
		return _last_count

def _check_query_match_last(_last_count, date, where, opt_filter=None):
	return _last_count is not None and _last_count[0]==(date, opt_filter, [w.where for w in where])