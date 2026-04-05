from io import StringIO
import pandas as pd
import pathlib
import pytest
import sys

import openpolicedata as opd
from openpolicedata import data
from openpolicedata.exceptions import OPD_FutureError, OPD_MinVersionError

sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_for_dataset

@pytest.fixture()
def log_stream():
    stream = StringIO()
    yield stream
    stream.truncate(0)
    stream.seek(0)
    assert len(stream.getvalue()) == 0

@pytest.fixture()
def logger(log_stream):
    logger = opd.log.get_logger()
    # Redirect handler output so that it can be checked
    logger.handlers[0].setStream(log_stream)

    yield logger
    for handler in logger.handlers:
        if handler.name != opd.log.stream_handler_name:
            logger.removeHandler(handler)


@pytest.mark.parametrize('ver', ["0.0", pd.NA])
def test_check_version_good(datasets, ver):
	ds = datasets.iloc[0].copy()

	ds["min_version"] = ver
	data._check_version(ds)

@pytest.mark.parametrize('ver, err', [("-1", OPD_FutureError), ("100000.0", OPD_MinVersionError)])
def test_check_version_bad(datasets, ver, err):
	ds = datasets.iloc[0].copy()
	ds["min_version"] = ver
	with pytest.raises(err):
		data._check_version(ds)



def test_not_verbose(logger, log_stream):
	source = 'Lansing'
	table = "OFFICER-INVOLVED SHOOTINGS"
	if check_for_dataset(source, table):
		src = opd.Source(source)
		table = src.load(table, 'MULTIPLE')
		assert len(log_stream.getvalue()) == 0


def test_verbose(logger, log_stream):
	source = 'Lansing'
	table = "OFFICER-INVOLVED SHOOTINGS"
	if check_for_dataset(source, table):
		src = opd.Source(source)
		table = src.load(table, 'MULTIPLE', verbose=True)
		assert len(log_stream.getvalue())>0


def test_table_type_warning(all_datasets):
	sources = all_datasets.copy().iloc[0]
	sources["TableType"] = "TEST"
	with pytest.warns(UserWarning):
		data.Table(sources)

@pytest.mark.parametrize('source, table, year', [('Phoenix', "OFFICER-INVOLVED SHOOTINGS", 2022), 
				('Orlando', "OFFICER-INVOLVED SHOOTINGS", 2022), ('Indianapolis', "OFFICER-INVOLVED SHOOTINGS", 2022),
				('Philadelphia', "COMPLAINTS - BACKGROUND", 2018)])
def test_format_date_false(source, table, year):
	if check_for_dataset(source, table):
		src = opd.Source(source)
		table = src.load(table, year, format_date=False, nrows=1)
		# Confirm date has not been formatted
		assert isinstance(table.table[table.date_field].iloc[0],str)

# DO-NOT-REMOVE: Do not remove the below datasets when searching for already run datasets for testing of all untested sources
@pytest.mark.parametrize('source, table, year,url', [('Denver', "OFFICER-INVOLVED SHOOTINGS", 2022,'https://raw.githubusercontent.com/openpolicedata/opd-datasets/main/data/Colorado_Denver_OFFICER-INVOLVED_SHOOTINGS.csv'), 
				('Sparks', "OFFICER-INVOLVED SHOOTINGS", 2022, None),  
				('Louisville', "TRAFFIC STOPS", ['2018-12-29', '2019-01-01'], 'LMPD_STOPS_DATA_(2)')])
def test_format_date_false_not_allowed(source, table, year, url):
	if check_for_dataset(source, table):
		src = opd.Source(source)
		with pytest.raises(ValueError, match='Dates cannot be filtered'):
			src.load(table, year, format_date=False, nrows=1, url=url)