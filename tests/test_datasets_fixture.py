import os
import pandas as opd
import pandas as pd
import pytest
import shutil
from test_utils import get_changed_rows, get_datasets, get_line_numbers

import sys
sys.path.append('../openpolicedata')
import openpolicedata as opd

def test_get_line_numbers():
    file = 'git_diff.txt' if os.path.exists('git_diff.txt') else os.path.join('tests','git_diff.txt')
    with open(file,'r', encoding='utf-16-le') as f:
        result = f.read()

    added_lines_file_number = get_line_numbers(result)

    assert added_lines_file_number==[64, 85, 86, 96, 112, 113]


def test_changerows_and_csvfile():
    with pytest.raises(ValueError, match='.*does not appear to be an Git repository.*'):
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
    line1[1] = 'TEST: DELETEME'
    with open(csvfile,'a') as f:
        f.write(','.join(line1))

    try:
        df_all = get_datasets(csvfile=csvfile)
        df = get_datasets(use_changed_rows=True)
        assert len(df) < len(df_all)
        pd.testing.assert_series_equal(df_all.iloc[-1], df.iloc[-1], check_names=False)
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
    assert df_true.equals(df)


def test_default():
    df_true = opd.datasets.query()
    df = get_datasets()
    assert(df_true.equals(df))


if __name__ == "__main__":
    test_changerows_and_csvfile()