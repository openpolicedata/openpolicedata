import pytest

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
from openpolicedata import data, data_loaders
from openpolicedata.defs import DataType, TableType
from openpolicedata.exceptions import OPD_DataUnavailableError, OPD_TooManyRequestsError,  \
	OPD_MultipleErrors, OPD_arcgisAuthInfoError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError
import openpolicedata as opd
from datetime import datetime
import pandas as pd
from time import sleep
import warnings
import os

import pathlib
import sys
sys.path.append(pathlib.Path(__file__).parent.resolve())
from test_utils import check_for_dataset, update_outages, user_request_skip

base_sleep_time = 0.1

# Set Arcgis data loader to validate queries with arcgis package if installed
data_loaders._verify_arcgis = True

log_filename = f"pytest_url_errors_{datetime.now().strftime('%Y%m%d_%H')}.txt"
log_folder = os.path.join(".","data/test_logs")

outages_file = os.path.join("..","opd-data","outages.csv")

warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError)





@pytest.mark.parametrize('source,year,table_type,nrows,agency', [
	("Virginia",2020,"STOPS", 2000, "Fairfax County Police Department"), # CKAN
	("Philadelphia", 2021, "STOPS", 1000, None),  # Carto
	("Fairfax County",2016,"ARRESTS", 1000, None),  # ArcGIS
	("Norristown", 2016, "USE OF FORCE",100, None), # Excel
	("Denver", "MULTIPLE", "OFFICER-INVOLVED SHOOTINGS", 50, None) # CSV
])
def test_load_gen(source, year, table_type, nrows, agency):
	raise NotImplementedError("This should not run. Needs to be broken out...")
	if check_for_dataset(source, table_type):
		src = data.Source(source)
		max_iter = 10
		df = src.load(table_type, year, agency=agency, nrows=max_iter*nrows).table
		with warnings.catch_warnings():
			warnings.filterwarnings("ignore",category=RuntimeWarning)
			df = df.convert_dtypes()

		offset = 0
		k = 0
		for t in src.load_iter(table_type, year, nbatch=nrows, force=True, agency=agency):
			df_cur = df.iloc[offset:offset+len(t.table)].reset_index(drop=True)
			df2 = t.table.copy()
			if set(df.columns)!=set(df2.columns):
				# Expecting that a column was not returned because it is empty in the subset requested in this iteration
				assert len(df2.columns)<len(df.columns)
				missing_columns = list(set(df.columns).difference(set(df2.columns)))
				for col in missing_columns:
					assert df_cur[col].isnull().all()
					df_cur = df_cur.drop(columns=col)
			# Assure columns are in proper order
			assert set(df_cur.columns)==set(df2.columns)
			df2 = df2[df_cur.columns]
			# Ensure that dtypes match
			df2 = df2.astype(df_cur.dtypes.to_dict())
			assert df2.equals(df_cur)
			offset+=len(t.table)
			k+=1
			if k>=max_iter:
				break


