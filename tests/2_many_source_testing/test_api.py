from datetime import datetime

import numpy as np
from urllib.parse import urlparse

import pandas as pd

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import pytest

from openpolicedata import defs

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_load_for_datasets, shuffle

@pytest.fixture(scope='module')
def api_datasets(remaining_datasets, is_api):
	return remaining_datasets[is_api]

@pytest.fixture(scope='module')
def no_datefield(api_datasets):
	return api_datasets['date_field'].isnull()


@pytest.fixture(scope='module')
def small_datasets(api_datasets, no_datefield):
	is_small = api_datasets['TableType'].str.contains('COMPLAINTS') | \
		api_datasets['TableType'].str.contains('SHOOTINGS') | \
		api_datasets['TableType'].str.contains('USE OF FORCE') | \
		api_datasets['TableType'].isin([defs.TableType.DISCIPLINARY_RECORDS, defs.TableType.EMPLOYEE, defs.TableType.DEATHS_IN_CUSTODY,
									defs.TableType.LAWSUITS, defs.TableType.POINTING_WEAPON, defs.TableType.VEHICLE_PURSUITS])
	return is_small & (~no_datefield)

@pytest.fixture(scope='module')
def large_datasets(no_datefield, small_datasets):
	return (~no_datefield) & (~small_datasets)

def test_all_datasets_tested(no_datefield, small_datasets, large_datasets):
	x = (no_datefield + small_datasets + large_datasets)!=1
	numleft = x.sum()
	assert numleft==0, f'{numleft} datasets not used or used more than once'


def test_load_no_date_field(api_datasets, no_datefield, source, start_idx, skip, query={}):
	check_load_for_datasets(api_datasets[no_datefield],  skip, start_idx, source, query, nrows=100)


def test_load_small_dataset(api_datasets, small_datasets, source, start_idx, skip, query={}):
	nrows = 10000
	def set_date(dataset, src, table_type):
		start = dataset['coverage_start'] + pd.Timedelta(days=1) if dataset['coverage_start']<dataset['coverage_end'] else dataset['coverage_start']
		if dataset['Year']==defs.MULTI and len(src.filter(table_type))==1:
			end = pd.Timestamp(datetime.now()).floor('D')
		else:
			end = dataset['coverage_end'] - pd.Timedelta(days=1) if dataset['coverage_start']<dataset['coverage_end'] else dataset['coverage_end']
		return [start, end]
	
	def check_date(dataset, table, date):
		assert len(table.table)<=nrows
		assert table.table[dataset['date_field']].min() >= date[0]
		assert table.table[dataset['date_field']].max() < date[1]+pd.Timedelta(days=1)
	
	check_load_for_datasets(api_datasets[small_datasets],  skip, start_idx, source, query, testfcn=check_date, datefcn=set_date, nrows=nrows)


def test_load_large_dataset(api_datasets, large_datasets, source, start_idx, skip, query={}):
	raise NotImplementedError('Add date filter')
	check_load_for_datasets(api_datasets[large_datasets],  skip, start_idx, source, query, testfcn=check_date, nrows=100)