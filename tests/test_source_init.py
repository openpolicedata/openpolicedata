import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import exceptions
import openpolicedata as opd

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_for_dataset

def test_source_bad_source_name():
	source_name = "BAD"
	with pytest.raises(ValueError, match='No sources found'):
		opd.Source(source_name)
		
@pytest.mark.parametrize('src_name, state, agency', [('ERROR',None,None), ('ERROR','Arizona',None), ('Chandler','Arizona', 'ERROR'),
													 ('Chandler','ERROR', None)])
def test_no_source_match(all_datasets, src_name, state, agency):
	with pytest.raises(ValueError):
		data.Source(src_name, state, agency)

def test_single_agency(all_datasets):
	if check_for_dataset('Chandler', opd.defs.TableType.ARRESTS):
		data.Source('Chandler')

def test_multiple_agency_same_source_name_error(all_datasets):
	if check_for_dataset('Contra Costa County', opd.defs.TableType.STOPS):
		with pytest.raises(exceptions.MultiAgencySourceError):
			data.Source('Contra Costa County')

@pytest.mark.parametrize('agency', ['MULTIPLE', 'Contra Costa County'])
def test_multiple_agency_same_source_name(all_datasets, agency):
	if check_for_dataset('Contra Costa County', opd.defs.TableType.STOPS):
		data.Source('Contra Costa County', agency=agency)
