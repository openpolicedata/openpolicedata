# content of conftest.py
# https://docs.pytest.org/en/latest/example/simple.html
import os

import subprocess
import pandas as pd
import numpy as np

import sys
sys.path.append('../openpolicedata')
import openpolicedata as opd


# if the --use-changed-rows is specified in the test 
# make a table in the comments  with the columns --use-changed-rows and --csvfile with the true and false combinations for the rows
# | --use-changed-rows  | use_csvfile | Results                                                                                       |
# |---------------------|-------------|-----------------------------------------------------------------------------------------------|
# | True                | True        | Throw an error because it is ambiguous                                                        |
# | True                | False       | Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed |
# | False               | True        | Use the user specified csv file for the opd_source_table                                      |
# | False               | False       | Use default github opd_source_table.csv                                                       |
# 
def get_datasets(csvfile=None,use_changed_rows=False):
    if csvfile is None:
      use_csvfile = False
    if use_changed_rows==True and use_csvfile==True:
        raise ValueError("Both --use-changed-rows and --csvfile options were provided, which is ambiguous.")
    elif use_changed_rows==True and use_csvfile==False:
        # Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed
        added_lines_datasets = get_changed_rows('../opd-data','opd_source_table.csv')
        return added_lines_datasets
    elif use_changed_rows==False and use_csvfile==True:
        # Use the user specified csv file for the opd_source_table
        opd.datasets.datasets = opd.datasets._build(csvfile)
        return opd.datasets.query()
    elif use_changed_rows==False and use_csvfile==False:
        # Use default github opd_source_table.csv
        return opd.datasets.query()
    else:
        raise ValueError("There is a logic error in get_datasets. This line should never run.")
    
    raise ValueError("There is a logic error in get_datasets. This line should never run.")


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

