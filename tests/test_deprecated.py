if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')

import os
import pandas as pd
import warnings
import openpolicedata as opd
from openpolicedata.defs import TableType
from openpolicedata.deprecated._decorators import deprecated, input_swap
from openpolicedata.deprecated._pandas import DeprecationHandlerDataFrame, DeprecationHandlerSeries
import pytest

@pytest.fixture(scope='module')
def df_compat():
	compat_versions_file = 'https://github.com/openpolicedata/opd-data/raw/main/compatibility/compat_versions.csv'
	return pd.read_csv(compat_versions_file, dtype=str)

@input_swap([0,1], ['table_type','year'], [TableType, {'values':[opd.defs.NA, opd.defs.MULTI], 'types':[list, int]}], opt1=None)
def fswap(table_type,year):
	if table_type:
		TableType(table_type)
	assert year in [opd.defs.NA, opd.defs.MULTI] or isinstance(year,int) or isinstance(year,list)

@input_swap([0,1], ['table_type','year'], [TableType, {'values':[opd.defs.NA, opd.defs.MULTI], 'types':[list, int]}], error=True, opt1=None)
def fswap_error(table_type,year):
	fswap(table_type,year)

class SwapClass:
	@input_swap([1,2], ['table_type','year'], [TableType, {'values':[opd.defs.NA, opd.defs.MULTI], 'types':[list, int]}], error=True, opt1=None)
	def fswap_error(self, table_type, year, arg3, arg4):
		fswap(table_type,year)

@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_no_inputswap(year, table_type):
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		fswap(table_type, year)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_warning(year, table_type):
	with pytest.deprecated_call():
		fswap(year, table_type)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_keyword_warning(year, table_type):
	with pytest.deprecated_call():
		fswap(year, table_type=table_type)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_singleinput_warning(year, table_type):
	with pytest.deprecated_call():
		fswap(year)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_error(year, table_type):
	with pytest.raises(ValueError, match='have been swapped'):
		fswap_error(year, table_type)

@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_class_error(year, table_type):
	obj = SwapClass()
	with pytest.raises(ValueError, match='have been swapped'):
		obj.fswap_error(year, table_type)

@deprecated("MSG")
def fdep():
	pass


def test_datasets_no_civilian_tabletypes():
	df = opd.datasets.query()
	assert not df["TableType"].str.contains("CIVILIAN").any()


def test_datasets_get_datatype():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df["DataType"]
	assert isinstance(s, pd.Series)


def test_datasets_iloc_single_table_type():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x=="TableType"][0]
	assert isinstance(df.iloc[:, k], DeprecationHandlerSeries)

def test_datasets_iloc_single_table_type_subset_no_subject():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x=="TableType"][0]
	s = df.iloc[:2, k]
	if not s.str.contains("SUBJECT").any():
		assert isinstance(s, pd.Series)

def test_datasets_iloc_single_table_type_subset_subject():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x=="TableType"][0]
	s = df.iloc[31:34, k]
	if s.str.contains("SUBJECT").any():
		assert isinstance(s, DeprecationHandlerSeries)


def test_datasets_iloc_single_not_table_type():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x!="TableType"][0]
	assert isinstance(df.iloc[:, k], pd.Series)

def test_datasets_iloc_single_row():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.iloc[0], pd.Series)


def test_datasets_loc_single_table_type():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.loc[:, "TableType"], DeprecationHandlerSeries)

def test_datasets_loc_single_table_type_subset_no_subject():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df.loc[:2, "TableType"]
	if not s.str.contains("SUBJECT").any():
		assert isinstance(s, pd.Series)

def test_datasets_loc_single_table_type_subset_subject():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df.loc[31:34, "TableType"]
	if s.str.contains("SUBJECT").any():
		assert isinstance(s, DeprecationHandlerSeries)


def test_datasets_loc_single_not_table_type():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.loc[:, "DataType"], pd.Series)

def test_datasets_loc_single_row():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.loc[0], pd.Series)


def test_pandas_query_no_subject():
	df = opd.datasets.query(state="Virginia")
	assert isinstance(df, pd.DataFrame)


def test_pandas_query_has_subject():
	df = opd.datasets.query(state="California")
	assert isinstance(df, DeprecationHandlerDataFrame)


@pytest.mark.parametrize("y", [7, 13, range(0,8), [6, 7,13], slice(0,8)])
@pytest.mark.filterwarnings("error::DeprecationWarning")
def test_datasets_iloc_no_warning(all_datasets, y):
	all_datasets.iloc[0,y]


@pytest.mark.parametrize("x", [0, [0,1], range(0,2)])
@pytest.mark.filterwarnings("error::DeprecationWarning")
def test_datasets_iloc_single_input_no_warning(all_datasets, x):
	all_datasets.iloc[x]

@pytest.mark.parametrize("y", [8, 10, [8,9], range(8,12), [1,8], range(6,10), slice(8,10), slice(1,10,2)])
def test_datasets_iloc_warning(all_datasets, y):
	with pytest.deprecated_call():
		all_datasets.iloc[0,y]

if __name__ == "__main__":
	csvfile = None
	csvfile = "C:\\Users\\matth\\repos\\opd-data\\opd_source_table.csv"
	test_pandas_query_tabletype_subject(csvfile,None,None,None,None)
	test_pandas_query_tabletype_no_subject(csvfile,None,None,None,None)