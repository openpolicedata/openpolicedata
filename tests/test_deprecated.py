if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
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

@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_no_inputswap(year, table_type):
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		fswap(table_type, year)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_warning(year, table_type):
	with pytest.warns(DeprecationWarning):
		fswap(year, table_type)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_keyword_warning(year, table_type):
	with pytest.warns(DeprecationWarning):
		fswap(year, table_type=table_type)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_singleinput_warning(year, table_type):
	with pytest.warns(DeprecationWarning):
		fswap(year)


@pytest.mark.parametrize("year", [2019, [2019, 2020], opd.defs.NA, opd.defs.MULTI])
@pytest.mark.parametrize("table_type", [TableType.ARRESTS, str(TableType.ARRESTS)])
def test_inputswap_error(year, table_type):
	with pytest.raises(ValueError, match='have been swapped'):
		fswap_error(year, table_type)


@deprecated("MSG")
def fdep():
	pass

@pytest.mark.parametrize("func", [fdep, datasets_query])
def test_deprecated_decorator(func):
	with pytest.warns(DeprecationWarning):
		func()


@pytest.mark.parametrize("type1, type2", [("COMPLAINTS - SUBJECTS", 'COMPLAINTS - CIVILIANS'),
										  ("USE OF FORCE - SUBJECTS/OFFICERS", 'USE OF FORCE - CIVILIANS/OFFICERS')])
def test_deprecated_enums(type1, type2):
	with pytest.warns(DeprecationWarning):
		assert TableType(type1) == TableType(type2)


def test_datasets_no_civilian_tabletypes():
	df = opd.datasets.query()
	assert not df["TableType"].str.contains("CIVILIAN").any()


def test_datasets_equals_civilian():
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df["TableType"]
	assert isinstance(s, DeprecationHandlerSeries)
	with pytest.warns(DeprecationWarning):
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

	with pytest.warns(DeprecationWarning):
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
	with pytest.warns(DeprecationWarning):
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
	with pytest.warns(DeprecationWarning):
		t = opd.datasets.get_table_types(contains="- CIVILIANS")
	assert len (t)>0


def test_tabletype_contains_no_subject():
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		t = opd.datasets.get_table_types(contains="- SUBJECTS")
	assert len (t)>0

def test_source_table_not_deprecated():
	assert not check_compat_source_table(cur_ver='100.0')[0]

def test_source_table_bad_df_compat():
	assert not check_compat_source_table(df_compat=1)[0]

def test_source_table_deprecated():
	with pytest.warns(DeprecationWarning):
		loaded, df = check_compat_source_table(cur_ver='0.0')
	assert loaded
	assert len(df)==1  # Number of rows in 1st test source table

def test_source_table_fail_not_req(df_compat):
	df_compat = df_compat.copy(deep=True)
	df_compat.loc[0, 'csv_name'] = '?!~notafile.csv'
	with pytest.warns(DeprecationWarning):
		loaded, df = check_compat_source_table(df_compat=df_compat, cur_ver='0.0')
	assert loaded
	assert len(df)==2  # Number of rows in 2nd test source table

def test_source_table_fail_req(df_compat):
	df_compat = df_compat.copy(deep=True)
	df_compat.loc[1, 'csv_name'] = '?!~notafile.csv'
	with pytest.raises(opd.exceptions.CompatSourceTableLoadError):
		check_compat_source_table(df_compat=df_compat, cur_ver='0.0.1')


if __name__ == "__main__":
	csvfile = None
	csvfile = "C:\\Users\\matth\\repos\\opd-data\\opd_source_table.csv"
	test_pandas_query_tabletype_subject(csvfile,None,None,None,None)
	test_pandas_query_tabletype_no_subject(csvfile,None,None,None,None)