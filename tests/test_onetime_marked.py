# These are tests that run very slow and are rarely run

from datetime import datetime
import os
import pandas as pd
import time
import warnings

import openpolicedata as opd
from test_utils import update_outages

import pytest

from openpolicedata.exceptions import *
warn_errors = (OPD_DataUnavailableError, OPD_SocrataHTTPError, OPD_FutureError, OPD_MinVersionError)

outages_file = os.path.join("..","opd-data","outages.csv")

def user_request_skip(datasets, i, start_idx, source):
	if source != None and datasets.iloc[i]["SourceName"] != source:
		return True
     
     # User requested to start at start_idx
	if i<start_idx:
		return True
	
	return False

@pytest.mark.onetime(reason="This test loads EVERY CSV file and to test that get_count is accurate")
def test_csv_get_count(all_datasets, source, start_idx):
    datasets = all_datasets
    caught_exceptions = []
    caught_exceptions_warn = []

    for i in range(len(datasets)):
        if datasets.iloc[i]['DataType']!=opd.defs.DataType.CSV or user_request_skip(datasets, i, start_idx, source) or \
            'zip' in datasets.iloc[i]['URL']:
            continue

        srcName = datasets.iloc[i]["SourceName"]
        state = datasets.iloc[i]["State"]
        src = opd.data.Source(srcName, state=state)

        table_print = datasets.iloc[i]["TableType"]
        now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
        print(f"{now} Testing {i} of {len(datasets)-1}: {srcName} {table_print} table")

        # Handle cases where URL is required to disambiguate requested dataset
        ds_filter, _ = src._Source__filter_for_source(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], None, None, errors=False)
        url = datasets.iloc[i]['URL'] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None
        id = datasets.iloc[i]['dataset_id'] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None

        try:
            t = time.time()
            count = src.get_count(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], 
                    url=url, id=id)
            print(f"Count: {time.time() - t}")
            t = time.time()
            table = src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], pbar=False, 
                    url=url, id=id)
            print(f"Load: {time.time() - t}")
        except warn_errors as e:
            e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"])
            update_outages(outages_file, datasets.iloc[i], True, e)
            caught_exceptions_warn.append(e)
            continue
        except (OPD_TooManyRequestsError, OPD_arcgisAuthInfoError) as e:
            # Catch exceptions related to URLs not functioning
            e.prepend(f"Iteration {i}", srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"])
            update_outages(outages_file, datasets.iloc[i], True, e)
            caught_exceptions.append(e)
            continue
        except:
            raise

        assert count == len(table.table)

    if len(caught_exceptions)==1:
        raise caught_exceptions[0]
    elif len(caught_exceptions)>0:
        msg = f"{len(caught_exceptions)} URL errors encountered:\n"
        for e in caught_exceptions:
            msg += "\t" + e.args[0] + "\n"
        raise OPD_MultipleErrors(msg)

    for e in caught_exceptions_warn:
        warnings.warn(str(e))