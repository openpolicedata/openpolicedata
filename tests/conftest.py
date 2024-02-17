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

# Function to get changed rows
def get_changed_rows(repo_dir, file_name):
    cmd = f"git -C {repo_dir} diff HEAD -- {file_name}"
    result = subprocess.check_output(cmd, shell=True).decode('utf-8')
    lines= result.split("\n")
    
    # Initialize variables
    added_lines_file_number = []
    current_new_file_line_number = None

    for line in lines:
        # Detect hunk headers
        if line.startswith('@@'):
            # Extract new file line numbers
            _, new_file_range, _ = line.split(' ')[1:4]
            start_line = int(new_file_range.split(',')[0][1:])
            current_new_file_line_number = start_line - 1  # Adjust for 0-indexing and increment before use
        elif line.startswith('+') and not line.startswith('+++'):
            # Increment before adding because the line is part of the new file
            current_new_file_line_number += 1
            added_lines_file_number.append(current_new_file_line_number)
        elif line.startswith('-') or line.startswith(' '):
            # For unchanged or removed lines, only increment if it's not a removal from the new file
            if not line.startswith('-'):
                current_new_file_line_number += 1
    
    # extract the added lines from the csv file        
    csv_file = os.path.join("..",'opd-data','opd_source_table.csv')
       
    opd.datasets.datasets = opd.datasets._build(csv_file)
    datasets=opd.datasets.query()
    
    # convert the added_lines_file_number to dataframe indexes by subtracting 2 from each element and call the variable added_lines_dataframe_index
    added_lines_dataframe_index = [x-2 for x in added_lines_file_number]
    
    #return only the datasets that are in the added_lines_dataframe_index
    added_lines_datasets = datasets.iloc[added_lines_dataframe_index]
    
    return added_lines_datasets


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


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.csvfile
    if 'csvfile' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("csvfile", [option_value])
    else:
        metafunc.parametrize("csvfile", [None])

    option_value = metafunc.config.option.source
    if 'source' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("source", [option_value])
    else:
        metafunc.parametrize("source", [None])

    option_value = metafunc.config.option.last
    if 'last' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("last", [int(option_value)])
    else:
        metafunc.parametrize("last", [float('inf')])

    option_value = metafunc.config.option.skip
    if 'skip' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("skip", [option_value])
    else:
        metafunc.parametrize("skip", [None])

    option_value = metafunc.config.option.loghtml
    metafunc.parametrize("loghtml", [option_value])
    
    
# Pytest fixture to provide changed rows
# this needs to be global 
# TODO give an input to show the scope
@pytest.fixture
def changed_rows_fixture(request):
    print("I am running as a script: C")
    if request.config.getoption("--use-changed-rows"): #TODO move to conftest.py so can pass  changed_rows_fixture to any test
        # for fixtures can give scopes such as scope = session
        # test_opd_data1.py
        # replace test_source_download_limitable => datasets = get_changed_rows(repo_dir, file_name)
        
        repo_dir = "../opd-data"
        file_name = "opd_source_table.csv"
        return get_changed_rows(repo_dir, file_name)
    else:
        return None
    
@pytest.fixture
def csvfile(request):
    return request.config.getoption("--csvfile")