# Using PyTest

If using Linux first activate the virtual environment described in the main README
> `source .venv/bin/activate`

Then run PyTest at the project root by typing
> `python -m pytest` 

The above command will not run all the tests. To see which tests are skipped then run
> `python -m pytest -rs`

Before running the tests which validate the ability of the code to download data, the gold validation data needs to be created. To do this first create a directory named data in the sandbox folder.
> 'mkdir sandbox/data`

Next run the Jupyter notebook `denver_generate_test_data.py` to generate the data files.

Now run all the tests.

> `python -m pytest --runslow`