# Write Python code that adds pytest parameter to only test changed rows based on Git diff line #'s.
# To do this create a command line option --use-changed-rows for pytest. Add to the sections of the test code that use row selection such as skip, after that try to convert an argument to a fixture. 

# Assume all developers are using the same directory structure 
# The code should only identify local uncommitted changes.Changes that are deletions should be ignored
# This will only use uncommitted changes in working copy

import pytest
import subprocess
import csv
import io
import os
if __name__ == "__main__":
    from test_utils import get_datasets
else:
    from .test_utils import get_datasets

# Add a command line option to pytest
def test_some_functionality(use_changed_rows, csvfile):
    ds=get_datasets(csvfile,use_changed_rows=use_changed_rows)
    print("test_some_functionality datasets are:")
    print(ds)

if __name__ == "__main__":
    test_some_functionality(True,None)
    
# run with pytest -s tests/test_changed_rows.py --use-changed-rows
