if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')

import os
import pandas as pd
from packaging import version
import openpolicedata as opd
from openpolicedata.deprecated.source_table_compat import check_compat_source_table, github_compat_versions_file
import pytest

@pytest.fixture(scope='module')
def df_compat():
	return pd.read_csv(github_compat_versions_file, dtype=str)

@pytest.fixture(scope='module')
def compat_stub():
	idx = github_compat_versions_file.rfind('/')
	return github_compat_versions_file[:idx+1]

def test_source_table_not_deprecated():
	assert not check_compat_source_table(cur_ver='0.10.0')[0]

def test_source_table_bad_df_compat():
	assert not check_compat_source_table(df_compat=1)[0]

def test_source_table_deprecated_first(df_compat, compat_stub):
	ver = '0.0'
	with pytest.deprecated_call():
		loaded, df, loaded_file = check_compat_source_table(cur_ver=ver)
	assert loaded
	assert len(df)==1  # Number of rows in 1st test source table
	ver = version.parse(ver)
	avail_ver = df_compat['version'].apply(lambda x: ver==version.parse(x))
	assert loaded_file==compat_stub+df_compat.loc[avail_ver, 'csv_name'].iloc[0]
	
@pytest.mark.parametrize('ver',['0.0.1', '0.0.2', '0.8','0.8.1'])
def test_source_table_deprecated(ver, df_compat, compat_stub):
	with pytest.deprecated_call():
		loaded, df, loaded_file = check_compat_source_table(cur_ver=ver)
	assert loaded
	assert len(df)>1  # Only 1st table has length 1
	ver = version.parse(ver)
	# File should be first where the version is <= the compatibility version
	avail_ver = df_compat['version'].apply(lambda x: ver<=version.parse(x))
	assert loaded_file==compat_stub+df_compat.loc[avail_ver[avail_ver].index[0], 'csv_name']

@pytest.mark.parametrize('ver',['0.9','0.8.10'])
def test_source_table_deprecated_local_file(ver):  # This is meant for testing compatibility tables before they are publically available
	compat_versions_file = os.path.join(os.path.os.path.dirname(os.getcwd()), 'opd-data', 'compatibility', "compat_versions.csv")
	if not os.path.exists(compat_versions_file):
		return
	
	df_compat = pd.read_csv(compat_versions_file, dtype=str)
	gt_filename = os.path.join(os.path.os.path.dirname(os.getcwd()), 'opd-data', 'compatibility', df_compat.iloc[-1]['csv_name'])

	with pytest.deprecated_call():
		loaded, df, loaded_file = check_compat_source_table(cur_ver=ver, compat_versions_file=compat_versions_file)
	assert loaded
	assert len(df)>1000
	assert loaded_file == gt_filename

def test_source_table_fail_not_req(df_compat):
	df_compat = df_compat.copy(deep=True)
	df_compat.loc[0, 'csv_name'] = '?!~notafile.csv'
	with pytest.deprecated_call():
		loaded, df, loaded_file = check_compat_source_table(df_compat=df_compat, cur_ver='0.0')
	assert loaded
	assert len(df)==2  # Number of rows in 2nd test source table
	assert df_compat.loc[0, 'csv_name'] not in loaded_file

def test_source_table_fail_req(df_compat):
	df_compat = df_compat.copy(deep=True)
	df_compat.loc[1, 'csv_name'] = '?!~notafile.csv'
	with pytest.raises(opd.exceptions.CompatSourceTableLoadError):
		check_compat_source_table(df_compat=df_compat, cur_ver='0.0.1')
