# Using PyTest
When running pytest from the terminal from VSCode the `.env` file settings will not be used.  Therefore, the src directory needs to be added to the Python path. 

If using Linux first activate the virtual environment described in the main README
> `source .venv/bin/activate`
Next add the src directory to the Python path with
> `export PYTHONPATH=./src:${PYTHONPATH}` 

Then run PyTest at the project root by typing
> `pytest` 

The above command will not run all the tests. To see which tests are skipped then run
> `pytest -rs`

Before running the tests which validate the ability of the code to download data, the gold validation data needs to be created. To do this first create a directory named data in the sandbox folder.
> 'mkdir sandbox/data`

Next run the Jupyter notebook `denver_generate_test_data.py` to generate the data files.

Now run all the tests.

> `pytest --runslow`