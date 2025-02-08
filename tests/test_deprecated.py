if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')

import os
import pandas as pd
import warnings
import openpolicedata as opd
from openpolicedata.defs import TableType
from openpolicedata.deprecated._decorators import deprecated, input_swap
from openpolicedata.deprecated.datasetsCompat import datasets_query
from openpolicedata.deprecated._pandas import DeprecationHandlerDataFrame, DeprecationHandlerSeries
from openpolicedata.deprecated.source_table_compat import check_compat_source_table
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

@pytest.mark.parametrize("func", [fdep, datasets_query])
def test_deprecated_decorator(func):
	with pytest.deprecated_call():
		func()


@pytest.mark.parametrize("type1, type2", [("COMPLAINTS - SUBJECTS", 'COMPLAINTS - CIVILIANS'),
										  ("USE OF FORCE - SUBJECTS/OFFICERS", 'USE OF FORCE - CIVILIANS/OFFICERS')])
def test_deprecated_enums(type1, type2):
	with pytest.deprecated_call():
		assert TableType(type1) == TableType(type2)


def test_datasets_no_civilian_tabletypes():
	df = opd.datasets.query()
	assert not df["TableType"].str.contains("CIVILIAN").any()


def test_datasets_equals_civilian():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df["TableType"]
	assert isinstance(s, DeprecationHandlerSeries)
	with pytest.deprecated_call():
		t = s == "USE OF FORCE - CIVILIANS/OFFICERS"

	assert t.sum()>0
	s = df["TableType"][t]
	assert isinstance(s, pd.Series)
	assert len(s)>0
	assert len(df[t])>0


def test_datasets_equals_subjects():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df["TableType"]
	assert isinstance(s, DeprecationHandlerSeries)
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		t = s == "USE OF FORCE - SUBJECTS/OFFICERS"

	assert t.sum()>0
	assert len(df["TableType"][t])>0
	assert len(df[t])>0


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


def test_datasets_isin_civilians():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	with pytest.deprecated_call():
		df["TableType"].isin(["ARRESTS", "OFFICER-INVOLVED SHOOTINGS - CIVILIANS"])


def test_datasets_isin_subjects():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	with warnings.catch_warnings():
		warnings.simplefilter("error")
		df["TableType"].isin(["ARRESTS", "OFFICER-INVOLVED SHOOTINGS - SUBJECTS"])


def test_datasets_isin_DataType():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	with warnings.catch_warnings():
		warnings.simplefilter("error")
		df["DataType"].isin(["ARRESTS", "OFFICER-INVOLVED SHOOTINGS - SUBJECTS"])


def test_pandas_query_no_subject():
	df = opd.datasets.query(state="Virginia")
	assert isinstance(df, pd.DataFrame)


def test_pandas_query_has_subject():
	df = opd.datasets.query(state="California")
	assert isinstance(df, DeprecationHandlerDataFrame)


def test_pandas_query_tabletype_subject():
	with pytest.deprecated_call():
		df = opd.datasets.query(table_type="COMPLAINTS - CIVILIANS")
	assert isinstance(df, DeprecationHandlerDataFrame)
	assert len (df)>0


def test_pandas_query_tabletype_no_subject():
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		df = opd.datasets.query(table_type=opd.defs.TableType.COMPLAINTS_SUBJECTS)
	assert isinstance(df, pd.DataFrame)
	assert len (df)>0

def test_tabletype_contains_subject():
	with pytest.deprecated_call():
		t = opd.datasets.get_table_types(contains="- CIVILIANS")
	assert len (t)>0


def test_tabletype_contains_no_subject():
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		t = opd.datasets.get_table_types(contains="- SUBJECTS")
	assert len (t)>0

def test_source_table_not_deprecated():
	assert not check_compat_source_table(cur_ver='0.8.2')[0]

def test_source_table_bad_df_compat():
	assert not check_compat_source_table(df_compat=1)[0]

def test_source_table_deprecated():
	with pytest.deprecated_call():
		loaded, df = check_compat_source_table(cur_ver='0.0')
	assert loaded
	assert len(df)==1  # Number of rows in 1st test source table

@pytest.mark.parametrize('ver',['0.8','0.8.1'])
def test_source_table_deprecated_local_file(ver):  # This is meant for testing compatibility tables before they are publically available
	compat_versions_file = os.path.join(os.path.os.path.dirname(os.getcwd()), 'opd-data', 'compatibility', "compat_versions.csv")
	if not os.path.exists(compat_versions_file):
		return
	
	df_compat = pd.read_csv(compat_versions_file, dtype=str)
	df_compat.loc[2,'csv_name'] = os.path.join(os.path.os.path.dirname(os.getcwd()), 'opd-data', 'compatibility', "opd_source_table_20241120_v0.8.1.csv")

	with pytest.deprecated_call():
		loaded, df = check_compat_source_table(cur_ver=ver, df_compat=df_compat, compat_versions_file="")
	assert loaded
	assert len(df)>1000

def test_source_table_fail_not_req(df_compat):
	df_compat = df_compat.copy(deep=True)
	df_compat.loc[0, 'csv_name'] = '?!~notafile.csv'
	with pytest.deprecated_call():
		loaded, df = check_compat_source_table(df_compat=df_compat, cur_ver='0.0')
	assert loaded
	assert len(df)==2  # Number of rows in 2nd test source table

def test_source_table_fail_req(df_compat):
	df_compat = df_compat.copy(deep=True)
	df_compat.loc[1, 'csv_name'] = '?!~notafile.csv'
	with pytest.raises(opd.exceptions.CompatSourceTableLoadError):
		check_compat_source_table(df_compat=df_compat, cur_ver='0.0.1')

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

@pytest.mark.parametrize("x, y", [(None, 'Test'), ('Test', 'Test')])
def test_url_contains_warning(x,y):
	with pytest.deprecated_call():
		result,_ = opd.data._handle_deprecated_filters(url=x, url_contains=y, id=None, id_contains=None)

	assert result==y

@pytest.mark.parametrize("x, y", [(None, 'Test'), ('Test', 'Test')])
def test_id_contains_warning(x,y):
	with pytest.deprecated_call():
		_,result = opd.data._handle_deprecated_filters(url=None, url_contains=None, id=x, id_contains=y)

	assert result==y

def test_url_contains_error():
	with pytest.raises(ValueError):
		opd.data._handle_deprecated_filters(url='TEST', url_contains='TEST2', id=None, id_contains=None)

def test_id_contains_error():
	with pytest.raises(ValueError):
		opd.data._handle_deprecated_filters(url=None, url_contains=None, id='TEST', id_contains='TEST2')


def test_url_success():
	url = 'TEST'
	result,_ = opd.data._handle_deprecated_filters(url=url, url_contains=None, id=None, id_contains=None)

	assert result==url

def test_id_success():
	id = 'TEST'
	_, result = opd.data._handle_deprecated_filters(url=None, url_contains=None, id=id, id_contains=None)

	assert result==id

if __name__ == "__main__":
	csvfile = None
	csvfile = "C:\\Users\\matth\\repos\\opd-data\\opd_source_table.csv"
	test_pandas_query_tabletype_subject(csvfile,None,None,None,None)
	test_pandas_query_tabletype_no_subject(csvfile,None,None,None,None)