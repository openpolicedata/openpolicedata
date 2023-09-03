if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import pandas as pd
import warnings
import openpolicedata as opd
from openpolicedata.defs import TableType
from openpolicedata.deprecated.datasetsCompat import datasets_query
from openpolicedata.deprecated._pandas import DeprecationHandlerDataFrame, DeprecationHandlerSeries
import pytest

def get_datasets(csvfile):
    if csvfile != None:
        opd.datasets.datasets = opd.datasets._build(csvfile)

    return opd.datasets.datasets

def test_deprecated_enums(csvfile, source, last, skip, loghtml):
	t = TableType("COMPLAINTS - SUBJECTS")

	with pytest.warns(DeprecationWarning):
		assert TableType('COMPLAINTS - CIVILIANS') == t

	t = TableType("USE OF FORCE - SUBJECTS/OFFICERS")

	with pytest.warns(DeprecationWarning):
		assert TableType('USE OF FORCE - CIVILIANS/OFFICERS') == t


def test_datasets_query(csvfile, source, last, skip, loghtml):
	with pytest.warns(DeprecationWarning):
		datasets_query()


def test_datasets(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert not df["TableType"].str.contains("CIVILIAN").any()


def test_datasets_equals_civilian(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
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


def test_datasets_equals_subjects(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
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


def test_datasets_get_datatype(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df["DataType"]
	assert isinstance(s, pd.Series)


def test_datasets_iloc_single_table_type(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x=="TableType"][0]
	assert isinstance(df.iloc[:, k], DeprecationHandlerSeries)

def test_datasets_iloc_single_table_type_subset_no_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x=="TableType"][0]
	s = df.iloc[:2, k]
	if not s.str.contains("SUBJECT").any():
		assert isinstance(s, pd.Series)

def test_datasets_iloc_single_table_type_subset_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x=="TableType"][0]
	s = df.iloc[31:34, k]
	if s.str.contains("SUBJECT").any():
		assert isinstance(s, DeprecationHandlerSeries)


def test_datasets_iloc_single_not_table_type(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	k = [j for j,x in enumerate(df.columns) if x!="TableType"][0]
	assert isinstance(df.iloc[:, k], pd.Series)

def test_datasets_iloc_single_row(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.iloc[0], pd.Series)


def test_datasets_loc_single_table_type(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.loc[:, "TableType"], DeprecationHandlerSeries)

def test_datasets_loc_single_table_type_subset_no_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df.loc[:2, "TableType"]
	if not s.str.contains("SUBJECT").any():
		assert isinstance(s, pd.Series)

def test_datasets_loc_single_table_type_subset_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	s = df.loc[31:34, "TableType"]
	if s.str.contains("SUBJECT").any():
		assert isinstance(s, DeprecationHandlerSeries)


def test_datasets_loc_single_not_table_type(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.loc[:, "DataType"], pd.Series)

def test_datasets_loc_single_row(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	assert isinstance(df.loc[0], pd.Series)


def test_datasets_isin_civilians(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	with pytest.warns(DeprecationWarning):
		df["TableType"].isin(["ARRESTS", "OFFICER-INVOLVED SHOOTINGS - CIVILIANS"])


def test_datasets_isin_subjects(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	with warnings.catch_warnings():
		warnings.simplefilter("error")
		df["TableType"].isin(["ARRESTS", "OFFICER-INVOLVED SHOOTINGS - SUBJECTS"])


def test_datasets_isin_DataType(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query()
	assert isinstance(df, DeprecationHandlerDataFrame)

	with warnings.catch_warnings():
		warnings.simplefilter("error")
		df["DataType"].isin(["ARRESTS", "OFFICER-INVOLVED SHOOTINGS - SUBJECTS"])


def test_pandas_query_no_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query(state="Virginia")
	assert isinstance(df, pd.DataFrame)


def test_pandas_query_has_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	df = opd.datasets.query(state="California")
	assert isinstance(df, DeprecationHandlerDataFrame)


def test_pandas_query_tabletype_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	with pytest.warns(DeprecationWarning):
		df = opd.datasets.query(table_type="COMPLAINTS - CIVILIANS")
	assert isinstance(df, DeprecationHandlerDataFrame)
	assert len (df)>0


def test_pandas_query_tabletype_no_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		df = opd.datasets.query(table_type=opd.defs.TableType.COMPLAINTS_SUBJECTS)
	assert isinstance(df, pd.DataFrame)
	assert len (df)>0

def test_tabletype_contains_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	with pytest.warns(DeprecationWarning):
		t = opd.datasets.get_table_types(contains="- CIVILIANS")
	assert len (t)>0


def test_tabletype_contains_no_subject(csvfile, source, last, skip, loghtml):
	get_datasets(csvfile)
	with warnings.catch_warnings():
		warnings.simplefilter("error")
		t = opd.datasets.get_table_types(contains="- SUBJECTS")
	assert len (t)>0


if __name__ == "__main__":
	csvfile = None
	csvfile = "C:\\Users\\matth\\repos\\opd-data\\opd_source_table.csv"
	test_pandas_query_tabletype_subject(csvfile,None,None,None,None)
	test_pandas_query_tabletype_no_subject(csvfile,None,None,None,None)