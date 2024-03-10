# content of conftest.py
# https://docs.pytest.org/en/latest/example/simple.html
import os
import pytest

import subprocess
import csv
import io
import pandas as pd
import numpy as np

if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
import openpolicedata as opd



# # if the --use-changed-rows is specified in the test 
# # make a table in the comments  with the columns --use-changed-rows and --csvfile with the true and false combinations for the rows
# # | --use-changed-rows  | --csvfile | Results                                                                                       |
# # |---------------------|-----------|-----------------------------------------------------------------------------------------------|
# # | True                | True      | Throw an error because it is ambiguous                                                        |
# # | True                | False     | Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed |
# # | False               | True      | Use the user specified csv file for the opd_source_table                                      |
# # | False               | False     | Use default github opd_source_table.csv                                                       |
# # 
# def get_datasets(csvfile=None,use_changed_rows=False):
#     if use_changed_rows and csvfile:
#         raise ValueError("Both --use-changed-rows and --csvfile options were provided, which is ambiguous.")
#     elif use_changed_rows:
#         # Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed
#         #opd.datasets.datasets = opd.datasets._build('../opd-data/opd_source_table.csv')
#         repo_dir = "../opd-data"
#         file_name = "opd_source_table.csv"        
#         opd.datasets.datasets = get_changed_rows(repo_dir, file_name)
#     elif csvfile:
#         # Use the user specified csv file for the opd_source_table
#         opd.datasets.datasets = opd.datasets._build(csvfile)
#     else:
#         # Use default github opd_source_table.csv
#         opd.datasets.datasets = opd.datasets._build('opd_source_table.csv')

#     return opd.datasets.query()


# # Function to get changed rows
# def get_changed_rows(repo_dir, file_name):
#     cmd = f"git -C {repo_dir} diff HEAD -- {file_name}"
#     result = subprocess.check_output(cmd, shell=True).decode('utf-8')
#     lines= result.split("\n")
    
#     # Initialize variables
#     added_lines_file_number = []
#     current_new_file_line_number = None

#     for line in lines:
#         # Detect hunk headers
#         if line.startswith('@@'):
#             # Extract new file line numbers
#             _, new_file_range, _ = line.split(' ')[1:4]
#             start_line = int(new_file_range.split(',')[0][1:])
#             current_new_file_line_number = start_line - 1  # Adjust for 0-indexing and increment before use
#         elif line.startswith('+') and not line.startswith('+++'):
#             # Increment before adding because the line is part of the new file
#             current_new_file_line_number += 1
#             added_lines_file_number.append(current_new_file_line_number)
#         elif line.startswith('-') or line.startswith(' '):
#             # For unchanged or removed lines, only increment if it's not a removal from the new file
#             if not line.startswith('-'):
#                 current_new_file_line_number += 1
    
#     # extract the added lines from the csv file        
#     csv_file = os.path.join("..",'opd-data','opd_source_table.csv')
       
#     opd.datasets.datasets = opd.datasets._build(csv_file)
#     datasets=opd.datasets.query()
    
#     # convert the added_lines_file_number to dataframe indexes by subtracting 2 from each element and call the variable added_lines_dataframe_index
#     added_lines_dataframe_index = [x-2 for x in added_lines_file_number]
    
#     #return only the datasets that are in the added_lines_dataframe_index
#     added_lines_datasets = datasets.iloc[added_lines_dataframe_index]
    
#     return added_lines_datasets




def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--csvfile", action="store", default=None, help="Replace default opd_source_table.csv with an updated one for testing"
    )
    parser.addoption(
        "--source", action="store", default=None, help="Only test datasets from this source"
    )
    parser.addoption(
        "--last", action="store", default=None, help="Run only the last N datasets in tests"
    )
    parser.addoption(
        "--skip", action="store", default=None, help="Comma-separated list of sources to skip"
    )
    parser.addoption(
        "--loghtml", action="store", default=0, help="0 (default) or 1 indicating if URL warnings/errors should be logged"
    )

    parser.addoption("--use-changed-rows", action="store_true", help="Run tests only on changed rows")



def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)

# #TODO convert to fixture
# def pytest_generate_tests(metafunc):
#     option_value = metafunc.config.option.csvfile
#     if 'csvfile' in metafunc.fixturenames and option_value is not None:
#         metafunc.parametrize("csvfile", [option_value])
#     else:
#         metafunc.parametrize("csvfile", [None])

#     option_value = metafunc.config.option.source
#     if 'source' in metafunc.fixturenames and option_value is not None:
#         metafunc.parametrize("source", [option_value])
#     else:
#         metafunc.parametrize("source", [None])

#     option_value = metafunc.config.option.last
#     if 'last' in metafunc.fixturenames and option_value is not None:
#         metafunc.parametrize("last", [int(option_value)])
#     else:
#         metafunc.parametrize("last", [float('inf')])

#     option_value = metafunc.config.option.skip
#     if 'skip' in metafunc.fixturenames and option_value is not None:
#         metafunc.parametrize("skip", [option_value])
#     else:
#         metafunc.parametrize("skip", [None])

#     option_value = metafunc.config.option.loghtml
#     metafunc.parametrize("loghtml", [option_value])
# import pytest

# Define fixtures for each command line option
@pytest.fixture
def csvfile(request):
    return request.config.option.csvfile

@pytest.fixture
def source(request):
    return request.config.option.source

@pytest.fixture
def last(request):
    return int(request.config.option.last) if request.config.option.last is not None else float('inf')

@pytest.fixture
def skip(request):
    return request.config.option.skip

@pytest.fixture
def loghtml(request):
    return request.config.option.loghtml    
    
# Pytest fixture to provide changed row option
@pytest.fixture
def use_changed_rows(request):
    return request.config.option.use_changed_rows

