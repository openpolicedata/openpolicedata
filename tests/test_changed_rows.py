# Write Python code that adds pytest parameter to only test changed rows based on Git diff line #'s.
# To do this create a command line option --use-changed-rows for pytest. Add to the sections of the test code that use row selection such as skip, after that try to convert an argument to a fixture. 

# Assume all developers are using the same directory structure 
# The code should only identify local uncommitted changes.Changes that are deletions should be ignored
# This will only use uncommitted changes in working copy

import pytest
import subprocess
import csv
import io
import pytest

# Add a command line option to pytest
def test_some_functionality(changed_rows_fixture, csvfile, source, last, skip, loghtml):
    if changed_rows_fixture:
        # Logic to run test only for changed rows
        # Use changed_rows_fixture to get the list of changed rows
        repo_dir = "../opd-data"
        file_name = "opd_source_table.csv"
        #get_changed_rows(repo_dir, file_name)        
    else:
        # Regular test logic
        pass
print("I am running as a script: D and __name__ is ", __name__)
# only run this if running as a script


if __name__ == "__main__":
    repo_dir = "../opd-data"
    file_name = "opd_source_table.csv"
    print("I am running as a script: A")
    changed_rows = get_changed_rows(repo_dir, file_name)
    test_some_functionality(changed_rows, None, None, None, None, None)
    print("I am running as a script: B")
    print(changed_rows)
# source /home/user/cjc/openpolicedata/.venv/bin/activate
# run with pytest -s tests/test_changed_rows.py --use-changed-rows
# The -m option in Python is used to run library modules as scripts. When you run a Python module with -m, Python executes 
# the module's contents as the __main__ module, allowing you to run code that's inside a Python module directly 
# from the command line.