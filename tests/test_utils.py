import pandas as pd
import os
import subprocess
import warnings

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
    csvfile = m if csvfile and os.path.exists((m:=csvfile.replace('\\','/'))) else csvfile

    if use_changed_rows:
        # Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed
        if csvfile:
            # Check if git repo
            cmd =f'git -C {os.path.dirname(csvfile)} remote -v'
            result = subprocess.run(cmd, stdout=subprocess.PIPE).stdout.decode('utf-8')
            if "opd-data.git" not in result:
                 raise FileNotFoundError(f"{csvfile} does not appear to be an Git repository for opd-data")
            csv_path = os.path.dirname(csvfile)
        else:
            csv_path = os.path.join('..','opd-data')
        assert os.path.exists(csv_path)
        added_lines_datasets = get_changed_rows(csv_path, 'opd_source_table.csv')
        opd.datasets.reload(added_lines_datasets)
    elif not use_changed_rows and csvfile:
        assert os.path.exists(csvfile)
        # Use the user specified csv file for the opd_source_table
        opd.datasets.reload(csvfile)

    return opd.datasets.query()
    


def get_line_numbers(result):
    lines= result.split("\n")

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

    # convert the added_lines_file_number to dataframe indexes by subtracting 2 from each element and call the variable added_lines_dataframe_index
    added_lines_dataframe_index = [x-2 for x in added_lines_file_number]

    return added_lines_dataframe_index


# Function to get changed rows
def get_changed_rows(repo_dir, file_name):
    cmd = f"git -C {repo_dir} diff -- {file_name}"
    result = subprocess.check_output(cmd, shell=True).decode('utf-8')

    added_lines_dataframe_index = get_line_numbers(result)
    
    # extract the added lines from the csv file        
    csv_file = os.path.join(repo_dir, file_name)
       
    datasets = pd.read_csv(csv_file)
    
    #return only the datasets that are in the added_lines_dataframe_index
    added_lines_datasets = datasets.iloc[added_lines_dataframe_index]
    
    return added_lines_datasets

def check_for_dataset(source, table_type, warn=True):
	ds = opd.datasets.query(source_name=source, table_type=table_type)
	if len(ds):
		return True
	elif warn:
		warnings.warn(f"No data found for {source} {table_type}")
	return False