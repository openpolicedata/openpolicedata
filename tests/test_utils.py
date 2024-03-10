# content of conftest.py
# https://docs.pytest.org/en/latest/example/simple.html
import pytest
import os
import shutil
import subprocess
import pandas as pd

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
    if use_changed_rows==True and csvfile:
        raise ValueError("Both --use-changed-rows and --csvfile options were provided, which is ambiguous.")
    elif use_changed_rows==True and not csvfile:
        # Use the added rows in the local ../opd-data/opd_source_table.csv that have not been committed
        added_lines_datasets = get_changed_rows(os.path.join('..','opd-data'),'opd_source_table.csv')
        return added_lines_datasets
    elif use_changed_rows==False and csvfile:
        # Use the user specified csv file for the opd_source_table
        opd.datasets.datasets = opd.datasets._build(csvfile)
        return opd.datasets.query()
    elif use_changed_rows==False and not csvfile:
        # Use default github opd_source_table.csv
        return opd.datasets.query()
    
    raise ValueError("There is a logic error in get_datasets. This line should never run.")


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
    cmd = f"git -C {repo_dir} diff HEAD -- {file_name}"
    result = subprocess.check_output(cmd, shell=True).decode('utf-8')

    added_lines_dataframe_index = get_line_numbers(result)
    
    # extract the added lines from the csv file        
    csv_file = os.path.join(repo_dir, file_name)
       
    opd.datasets.datasets = opd.datasets._build(csv_file)
    datasets=opd.datasets.query()
    
    #return only the datasets that are in the added_lines_dataframe_index
    added_lines_datasets = datasets.iloc[added_lines_dataframe_index]
    
    return added_lines_datasets


def test_get_line_numbers():
    file = 'test_git_diff.txt' if os.path.exists('test_git_diff.txt') else os.path.join('tests','test_git_diff.txt')
    with open(file,'r', encoding='utf-16-le') as f:
        result = f.read()

    added_lines_file_number = get_line_numbers(result)

    assert added_lines_file_number==[64, 85, 86, 96, 112, 113]


def test_changerows_and_csvfile():
    with pytest.raises(ValueError, match='^Both --use-changed-rows'):
        get_datasets("test", True)



def test_changerows():
    csvfile = os.path.join('..','opd-data','opd_source_table.csv')
    tmp_file = csvfile.replace('.csv','_ORIGNAL_COPY.csv')  # Create modified file to ensure change can by reverted
    shutil.copy2(csvfile, tmp_file)

    with open(csvfile,'r') as f:  # Grab 1st row
        f.readline()
        line1 = f.readline()

    # Add new line to create a change
    line1 = line1.split(',')
    line1[0] = 'Ohio' if line1[0]!='Ohio' else 'Utah'
    with open(csvfile,'a') as f:
        f.write(','.join(line1))

    try:
        num_lines = len(pd.read_csv(csvfile))
        df = get_datasets(use_changed_rows=True)
        assert len(df.index) < num_lines
        assert df.index[-1]==num_lines-1
    except:
        raise
    finally:
        # Revert changes
        os.remove(csvfile)
        os.rename(tmp_file, csvfile)


def test_csvfile():
    csvfile = os.path.join('..','opd-data','opd_source_table.csv')
    df_true = opd.datasets._build(csvfile)
    df = get_datasets(csvfile=csvfile)
    assert(df_true.equals(df))


def test_default():
    df_true = opd.datasets.query()
    df = get_datasets()
    assert(df_true.equals(df))


if __name__ == "__main__":
    test_changerows_and_csvfile()