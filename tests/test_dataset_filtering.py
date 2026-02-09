import pytest
import re

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata.data_loaders import data_loader
import openpolicedata as opd
import pandas as pd

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_for_dataset


@pytest.mark.parametrize('date', [None, opd.defs.MULTI, opd.defs.NA, [pd.to_datetime('2025-02-03'), pd.to_datetime('2026-03-04')]])
def test_clean_date_input_same_input_output(date):
	assert date==data_loader._clean_date_input(date)

def test_clean_date_input_year():
	year = 2025
	assert data_loader._clean_date_input(year)==[pd.to_datetime(f'{year}-01-01'), pd.to_datetime(f'{year}-12-31')]

@pytest.mark.parametrize('date',[[2018,2020], [2018, '2020-02-15'], ['2020-02-15', 2023],
								 [2018, pd.to_datetime('2020-02-15')], [pd.to_datetime('2020-02-15'), 2023],
								 ['2018-04-05', '2020-02-15']])
def test_clean_date_input_list(date):
	clean_date = data_loader._clean_date_input(date)
	
	date[0] = f'{date[0]}-01-01' if isinstance(date[0], int) else date[0]
	date[1] = f'{date[1]}-12-31' if isinstance(date[1], int) else date[1]

	date = [pd.to_datetime(x) for x in date]

	assert clean_date==date


def test_clean_date_includes_time():
	date = ['2023-11-01T00:00:00','2023-12-31T23:59:59']
	with pytest.warns(UserWarning):
		clean_date = data_loader._clean_date_input(date)
	
	date = [pd.to_datetime(x).floor('24h') for x in date]

	assert clean_date==date


@pytest.mark.parametrize('date',[10000, 999, [999, '2020-02-15']])
def test_clean_date_input_year_out_of_range(date):
	with pytest.raises(ValueError):
		data_loader._clean_date_input(date)


def test_clean_date_input_bad_string_input():
	with pytest.raises(pd._libs.tslibs.parsing.DateParseError):
		data_loader._clean_date_input('BAD')

@pytest.mark.parametrize('date',[[2010], [2017,2018, 2019]])
def test_clean_date_input_list_not_length2(date):
	with pytest.raises(ValueError):
		data_loader._clean_date_input(date)

@pytest.mark.parametrize('date',[[2020, 2018], ['2020-02-15', 2018], [2023, '2020-02-15'],
								 [pd.to_datetime('2020-02-15'), 2018], [2023, pd.to_datetime('2020-02-15')],
								 ['2020-02-15', '2018-04-05']])
def test_clean_date_input_out_of_order(date):
	with pytest.raises(ValueError):
		data_loader._clean_date_input(date)

def test_find_datasets(all_datasets):
	table_type_enum = opd.defs.TableType.CALLS_FOR_SERVICE
	table_type = 'CALLS FOR SERVICE'
	loc = 'Phoenix'
	truth = all_datasets[(all_datasets['TableType']==table_type) & (all_datasets['SourceName']==loc)]

	src = data.Source(loc)
	ds = src._Source__find_datasets(table_type)
	pd.testing.assert_frame_equal(truth, ds)

	ds_enum = src._Source__find_datasets(table_type_enum)
	pd.testing.assert_frame_equal(ds_enum, ds)

def test_find_datasets_no_tabletype(all_datasets):
	loc = 'Phoenix'
	truth = all_datasets[all_datasets['SourceName']==loc]

	src = data.Source(loc)
	ds = src._Source__find_datasets(None)
	pd.testing.assert_frame_equal(truth, ds)

def test_find_datasets_src_input_df(all_datasets):
	table_type = 'CALLS FOR SERVICE'
	all_datasets = all_datasets.iloc[:int(len(all_datasets)/2)]
	truth = all_datasets[all_datasets['TableType']==table_type]

	src = data.Source('Phoenix')
	ds = src._Source__find_datasets(table_type, src=all_datasets)
	pd.testing.assert_frame_equal(truth, ds)

def test_find_datasets_src_input_series(all_datasets):
	table_type = 'CALLS FOR SERVICE'
	all_datasets = all_datasets.iloc[:int(len(all_datasets)/2)]
	truth = all_datasets[all_datasets['TableType']==table_type].iloc[0]

	src = data.Source('Phoenix')
	ds = src._Source__find_datasets(table_type, src=truth)
	pd.testing.assert_frame_equal(truth.to_frame().T, ds)


def test_filter_year_bad_table_type_no_error():
	src = data.Source('Asheville')
	ds = src.filter('FAKE', 2020)
	assert len(ds)==0


def test_filter_year_bad_table_type():
	src = data.Source('Asheville')
	with pytest.raises(ValueError, match='No source found for table type'):
		src.filter('FAKE', 2020, errors=True)


@pytest.mark.parametrize('loc, table_type,year', [('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE,2017), 
												  ('Tucson', opd.defs.TableType.ARRESTS, 2017),
												  ('Tucson', opd.defs.TableType.ARRESTS, 'MULTIPLE'),
												  ('Gilbert', opd.defs.TableType.EMPLOYEE, 'NONE')])
def test_filter_year_matches(loc, table_type, year):
	if check_for_dataset(loc, table_type):
		src = data.Source(loc)
		dataset = src.filter(table_type, year, errors=True)
		assert len(dataset)==1
		assert dataset.iloc[0]['Year']==year
		assert dataset.iloc[0]['TableType']==table_type


def test_filter_string_not_found():
	loc = 'Phoenix'
	table_type = opd.defs.TableType.CALLS_FOR_SERVICE
	if check_for_dataset(loc, table_type):
		src = data.Source(loc)
		with pytest.raises(ValueError, match='No source found with a Year field equal to'):
			src.filter(table_type, opd.defs.MULTI, errors=True)


def test_filter_string_not_found_no_error():
	loc = 'Phoenix'
	table_type = opd.defs.TableType.CALLS_FOR_SERVICE
	if check_for_dataset(loc, table_type):
		src = data.Source(loc)
		ds = src.filter(table_type, opd.defs.MULTI)
		assert len(ds)==0


def test_filter_year_not_found_single_multi():
	if check_for_dataset('Norristown', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Norristown')
		dataset = src.filter(opd.defs.TableType.USE_OF_FORCE, 2000, errors=True)
		assert len(dataset)==1
		assert dataset.iloc[0]['Year']==opd.defs.MULTI
		assert dataset.iloc[0]['TableType']==opd.defs.TableType.USE_OF_FORCE


def test_filter_year_nan_id():
	if check_for_dataset('Norristown', opd.defs.TableType.USE_OF_FORCE):
		year = 2019
		src = data.Source('Norristown')
		dataset = src.filter(opd.defs.TableType.USE_OF_FORCE, year, errors=True, id=pd.NA)
		assert len(dataset)==1
		assert dataset.iloc[0]['Year']==year
		assert dataset.iloc[0]['TableType']==opd.defs.TableType.USE_OF_FORCE


@pytest.mark.parametrize('truth,year',[[0, 2018], [1,2021]])
def test_filter_year_multi_multi(truth, year):
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		dataset = src.filter(table_type, year, errors=True)

		options = src.datasets[src.datasets['TableType']==table_type]
		pd.testing.assert_frame_equal(dataset, options.iloc[[truth]])


@pytest.mark.parametrize('loc, table, year',[
	('Asheville', opd.defs.TableType.USE_OF_FORCE, 2020),
	('Asheville', opd.defs.TableType.USE_OF_FORCE, opd.defs.MULTI),
	('Louisville', opd.defs.TableType.TRAFFIC, 2021),
	('Louisville', opd.defs.TableType.TRAFFIC, [2021, 2022]),
	('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE, [2016, 2018]),
	('Mesa', opd.defs.TableType.CALLS_FOR_SERVICE, opd.defs.MULTI)
	])
def test_filter_year_multi_multi_overlap_error(loc, table, year):
	if check_for_dataset(loc, table):
		src = data.Source(loc)
		with pytest.raises(ValueError, match="Requested dataset is ambiguous"):
			src.filter(table, year, errors=True)


@pytest.mark.parametrize('url, truth', [('APDUseOfForce',0),('2021',1)])
def test_filter_year_multi_multi_overlap_url_distinguish(url, truth):
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		dataset = src.filter(table_type, 2020, errors=True, url=url)

		options = src.datasets[src.datasets['TableType']==table_type]
		pd.testing.assert_frame_equal(dataset, options.iloc[[truth]])


def test_filter_year_bad_url():
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		with pytest.raises(ValueError, match='No source found with a URL field containing'):
			src.filter(table_type, 2020, errors=True, url='FAKE')


def test_filter_year_bad_url_no_error():
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		ds = src.filter(table_type, 2020, url='FAKE')
		assert len(ds)==0


def test_filter_year_multi_multi_update_years():
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		ds = src.datasets[src.datasets['TableType']==table_type]
		ds = ds[ds['coverage_end']==ds['coverage_end'].max()]
		year = src.datasets.loc[ds.index[0], 'coverage_end'].year

		# Update datasets to not include year to be requested so that it can be found with get_years call by filter fun
		src.datasets.loc[ds.index[0], 'coverage_end'] = src.datasets.loc[ds.index[0], 'coverage_end'] - pd.Timedelta(days=366)

		dataset = src.filter(table_type, year, errors=True)
		pd.testing.assert_frame_equal(dataset, src.datasets.loc[[ds.index[0]]])


def test_filter_year_multi_multi_out_of_range_error():
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		with pytest.raises(ValueError, match='No source found containing date'):
			src.filter(table_type, 1900, errors=True)


@pytest.mark.parametrize('year, count', [[1900, 0], [2020, 2]])
def test_filter_year_multi_multi_out_of_range_NO_error(year, count):
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		datasets = src.filter(table_type, year)
		assert len(datasets)==count


@pytest.mark.parametrize('id', ['ex94-c5ad','izhu-764k'])
def test_filter_multi_multi_id_distinguish(id):
	if check_for_dataset('Mesa', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Mesa')
		dataset = src.filter(opd.defs.TableType.CALLS_FOR_SERVICE, opd.defs.MULTI, id=id)
		assert id == dataset['dataset_id'].iloc[0]


def test_filter_year_bad_id():
	table_type = opd.defs.TableType.USE_OF_FORCE
	if check_for_dataset('Asheville', table_type):
		src = data.Source('Asheville')
		with pytest.raises(ValueError, match='No source found with a dataset id field containing'):
			src.filter(table_type, opd.defs.MULTI, id='FAKE', errors=True)


def test_filter_year_bad_url_no_id():
	table_type = opd.defs.TableType.CALLS_FOR_SERVICE
	if check_for_dataset('Mesa', table_type):
		src = data.Source('Mesa')
		ds = src.filter(table_type, opd.defs.MULTI, id='FAKE')
		assert len(ds)==0


def test_filter_no_multiple():
	src = data.Source('Phoenix')
	with pytest.raises(ValueError, match="No source found with a Year field equal to"):
		src.filter(opd.defs.TableType.CALLS_FOR_SERVICE, opd.defs.MULTI)


@pytest.mark.parametrize('loc, table_type,year', [('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE,2016), 
												  ('Tucson', opd.defs.TableType.ARRESTS, 'MULTIPLE'),
												  ('Gilbert', opd.defs.TableType.EMPLOYEE, 'NONE')])
def test_check_whether_to_filter_by_date_FALSE(loc, table_type, year):
	src = data.Source(loc)
	dataset = src.filter(table_type, year, errors=True)
	filter_by_date = opd.data._check_whether_to_filter_by_date(dataset, year)
	assert not filter_by_date


@pytest.mark.parametrize('year', [[2016, '2016-02-15'], ['2016-02-15', 2016],
								 ['2016-04-05', '2016-06-15']])
def test_check_whether_to_filter_by_date_TRUE_annual(year):
	if check_for_dataset('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Phoenix')
		dataset = src.filter(opd.defs.TableType.CALLS_FOR_SERVICE, year, errors=True)
		filter_by_date = opd.data._check_whether_to_filter_by_date(dataset, year)
		assert filter_by_date


@pytest.mark.parametrize('year', [[2016, '2017-01-01'], ['2015-12-31', 2016]])
def test_check_whether_to_filter_by_date_annual_FAIL(year):
	if check_for_dataset('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Phoenix')
		dataset = src.filter(opd.defs.TableType.CALLS_FOR_SERVICE, 2016, errors=True)
		with pytest.raises(ValueError, match='cannot be filtered for dates outside the year'):
			opd.data._check_whether_to_filter_by_date(dataset, year)
