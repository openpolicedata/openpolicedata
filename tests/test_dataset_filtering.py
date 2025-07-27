import pytest
import re

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
import openpolicedata as opd
import pandas as pd

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_for_dataset

def check_filter_results(dataset, filter_by_year_check, year, table_type):
	assert filter_by_year_check
	assert dataset['Year']==year
	assert dataset['TableType']==table_type

@pytest.mark.parametrize('date',[2019,'MULTIPLE','NONE',[2018,2020], [2018, '2020-02-15'], ['2020-02-15', 2023],
								 [2018, pd.to_datetime('2020-02-15')], [pd.to_datetime('2020-02-15'), 2023],
								 ['2018-04-05', '2020-02-15']])
def test_check_year_input(date):
	clean_date = data._check_date_input(date)
	if isinstance(date, list):
		date = [pd.to_datetime(x) if isinstance(x,str) else x for x in date]

	assert clean_date==date


@pytest.mark.parametrize('date',[10000, 999,'BAD','2020-02-15', pd.to_datetime('2020-02-15'), [999, '2020-02-15'],
								 [2020, 2018], ['2020-02-15', 2018], [2023, '2020-02-15'],
								 [pd.to_datetime('2020-02-15'), 2018], [2023, pd.to_datetime('2020-02-15')],
								 ['2020-02-15', '2018-04-05'], [2017,2018, 2019]])
def test_check_year_input_bad_input(date):
	with pytest.raises(ValueError):
		data._check_date_input(date)


@pytest.mark.parametrize('date',[[2018, '2020-02-15 14:04:23.999'], ['2020-02-15 14:04:23.999', 2023],
								 [2018, pd.to_datetime('2020-02-15 14:04:23.999')], [pd.to_datetime('2020-02-15 14:04:23.999'), 2023],
								 ['2018-04-05', '2020-02-15 14:04:23.999']])
def test_check_year_input_bad_input(date):
	with pytest.warns(UserWarning, match='Filtering is not currently possible for times'):
		data._check_date_input(date)


@pytest.mark.parametrize('loc, table_type,year', [('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE,2017), 
												  ('Tucson', opd.defs.TableType.ARRESTS, 2017),
												  ('Tucson', opd.defs.TableType.ARRESTS, 'MULTIPLE'),
												  ('Gilbert', opd.defs.TableType.EMPLOYEE, 'NONE')])
def test_filter_for_source_year_col_matches_req_year(loc, table_type, year):
	if check_for_dataset(loc, table_type):
		src = data.Source(loc)
		dataset, filter_by_year = src._Source__filter_for_source(table_type, year, None, None)
		check_filter_results(dataset, not filter_by_year, year, table_type)


@pytest.mark.parametrize('year', [2016,[2010, 2016], [pd.to_datetime('2010-02-15'), 2016],
								 [2016, '2016-12-31'], ['2016-01-01', 2016]])
def test_filter_for_source_single_year_no_filter_by_year(year):
	if check_for_dataset('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Phoenix')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, year, None, None)
		check_filter_results(dataset, not filter_by_year, 2016, opd.defs.TableType.CALLS_FOR_SERVICE)


@pytest.mark.parametrize('year', [[2016, '2016-02-15'], ['2016-02-15', 2016],
								 [2010, pd.to_datetime('2016-02-15')], ['2016-04-05', '2016-06-15']])
def test_filter_for_source_single_year_filter_by_year(year):
	if check_for_dataset('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Phoenix')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, year, None, None)
		check_filter_results(dataset, filter_by_year, 2016, opd.defs.TableType.CALLS_FOR_SERVICE)


@pytest.mark.parametrize('year', [2017,[2017, 2018], [2017, '2018-02-15'], ['2017-02-15', 2018],
								 [2018, pd.to_datetime('2018-02-15')], [pd.to_datetime('2017-02-15'), 2017],
								 ['2018-04-05', '2018-06-15'], 2010, [2010, 2011], 
								 ['2017-01-01', '2018-12-31'],[pd.to_datetime('2017-01-01'), pd.to_datetime('2018-12-31')],
								 2025, [2024, 2025]])
def test_filter_for_source_multi(year):
	if check_for_dataset('Norristown', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Norristown')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)
		check_filter_results(dataset, filter_by_year, opd.defs.MULTI, opd.defs.TableType.USE_OF_FORCE)


@pytest.mark.parametrize('year', [2018,[2018, 2019], [2017, '2020-12-15'], ['2018-02-15', 2019],
								 [2018, pd.to_datetime('2020-12-15')], [pd.to_datetime('2018-02-15'), 2018],
								 ['2018-04-05', '2018-06-15']])
def test_filter_for_source_multi_multiyear_select_first_dataset(year):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)
		exp_dataset = src.datasets[src.datasets['TableType']==opd.defs.TableType.USE_OF_FORCE].iloc[0]
		check_filter_results(dataset, filter_by_year, opd.defs.MULTI, opd.defs.TableType.USE_OF_FORCE)
		assert dataset['URL']==exp_dataset['URL']


@pytest.mark.parametrize('year', [2021,[2021, 2023], [2021, '2023-12-15'], ['2020-12-27', 2023],
								 [2021, pd.to_datetime('2023-12-15')], [pd.to_datetime('2020-12-27'), 2021],
								 ['2020-12-27', '2023-06-15']])
def test_filter_for_source_multi_multiyear_select_second_dataset(year):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, None, None)
		exp_dataset = src.datasets[src.datasets['TableType']==opd.defs.TableType.USE_OF_FORCE].iloc[1]
		check_filter_results(dataset, filter_by_year, opd.defs.MULTI, opd.defs.TableType.USE_OF_FORCE)
		assert dataset['URL']==exp_dataset['URL']


@pytest.mark.parametrize('url, truth', [('APDUseOfForce',0),('2021',1)])
def test_filter_for_source_multi_multiyear_url_distinguish(url, truth):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, 2020, url=url, id=None)
		urls = src.datasets[src.datasets['TableType']==opd.defs.TableType.USE_OF_FORCE]['URL'].tolist()
		check_filter_results(dataset, filter_by_year, opd.defs.MULTI, opd.defs.TableType.USE_OF_FORCE)
		assert url in dataset['URL']
		assert dataset['URL'] == urls[truth]
		assert url not in urls[1-truth]


@pytest.mark.parametrize('id, truth', [('ex94-c5ad',0),('izhu-764k',1)])
def test_filter_for_source_multi_multiyear_id_distinguish(id, truth):
	if check_for_dataset('Mesa', opd.defs.TableType.CALLS_FOR_SERVICE):
		src = data.Source('Mesa')
		dataset, filter_by_year = src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, opd.defs.MULTI, url=None, id=id)
		ids = src.datasets[src.datasets['TableType']==opd.defs.TableType.CALLS_FOR_SERVICE]['dataset_id'].tolist()
		check_filter_results(dataset, not filter_by_year, opd.defs.MULTI, opd.defs.TableType.CALLS_FOR_SERVICE)
		assert id == dataset['dataset_id']
		assert dataset['dataset_id'] == ids[truth]
		assert id != ids[1-truth]


@pytest.mark.parametrize('year, loc, table',[
	(2020, 'Asheville', opd.defs.TableType.USE_OF_FORCE),
	(opd.defs.MULTI, 'Asheville', opd.defs.TableType.USE_OF_FORCE),
	(2021, 'Louisville', opd.defs.TableType.TRAFFIC),
	([2021, 2022], 'Louisville', opd.defs.TableType.TRAFFIC),
	([2016, 2017], 'Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE)
	])
def test_filter_for_source_url_needed(year, loc, table):
	if check_for_dataset(loc, table):
		src = data.Source(loc)
		with pytest.raises(ValueError, match=re.compile(r"Requested dataset is ambiguous.*url=", re.DOTALL)):
			src._Source__filter_for_source(table, year, url=None, id=None)


@pytest.mark.parametrize('loc, table, year', 
	[('Mesa', opd.defs.TableType.CALLS_FOR_SERVICE, opd.defs.MULTI),
	('Phoenix', opd.defs.TableType.CALLS_FOR_SERVICE, [2017,2018])])
def test_filter_for_source_id_needed(loc, table, year):
	if check_for_dataset(loc, table):
		src = data.Source(loc)
		with pytest.raises(ValueError, match=re.compile(r"Requested dataset is ambiguous.*id=", re.DOTALL)):
			src._Source__filter_for_source(table, year, url=None, id=None)


@pytest.mark.parametrize('year',[2020, opd.defs.MULTI])
def test_filter_for_source_errors_eq_false(year):
	if check_for_dataset('Asheville', opd.defs.TableType.USE_OF_FORCE):
		src = data.Source('Asheville')
		dataset, _ = src._Source__filter_for_source(opd.defs.TableType.USE_OF_FORCE, year, url=None, id=None, errors=False)
		assert len(dataset)==2


def test_filter_for_source_bad_url():
	src = data.Source('Phoenix')
	with pytest.raises(ValueError, match="No source found with a URL field containing"):
		src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, 2017, 'BAD URL', None)


def test_filter_for_source_bad_id():
	src = data.Source('Phoenix')
	with pytest.raises(ValueError, match="No source found with a dataset id field containing"):
		src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, 2017, None, 'BAD ID')


def test_filter_for_source_bad_table_type():
	src = data.Source('Phoenix')
	with pytest.raises(ValueError, match="No source found for table type"):
		src._Source__filter_for_source('BAD TABLE TYPE', 2017, None, None)


def test_filter_for_source_no_multiple():
	src = data.Source('Phoenix')
	with pytest.raises(ValueError, match="No source found with a Year field equal to"):
		src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, opd.defs.MULTI, None, None)


def test_filter_for_source_no_year():
	src = data.Source('Phoenix')
	with pytest.raises(ValueError, match="No source found containing date"):
		src._Source__filter_for_source(opd.defs.TableType.CALLS_FOR_SERVICE, 1980, None, None)