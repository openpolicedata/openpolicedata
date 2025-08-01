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

@pytest.mark.parametrize("func", [fdep])
def test_deprecated_decorator(func):
	with pytest.deprecated_call():
		func()
