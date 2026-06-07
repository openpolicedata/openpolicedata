if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data
from openpolicedata import datasets
from openpolicedata.defs import MULTI, DataType
import openpolicedata as opd
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
from datetime import datetime
from time import sleep
import warnings
import os
import pandas as pd
import pytest

from test_utils import check_for_dataset, user_request_skip




@pytest.mark.parametrize('save,fname',[('to_feather','get_feather_filename'),
											('to_parquet','get_parquet_filename')])
def test_save_mixed_dtype_column(save,fname):
	if check_for_dataset('New York City', 'PEDESTRIAN STOPS'):
		src = data.Source("New York City")
		table = src.load('PEDESTRIAN STOPS', 2014, pbar=False)
	
		getattr(table, save)(mixed=True)

		filename = getattr(table, fname)()
		assert os.path.exists(filename)
		os.remove(filename)

