# Using PyTest
When running pytest from the terminal from VSCode the `.env` file settings will not be used.  Therefore, the src directory needs to be added to the Python path. 

If using Linux first activate the virtual environment described in the main README
> `source .venv/bin/activate`
Next add the src directory to the Python path with
> `export PYTHONPATH=./src:${PYTHONPATH}` 

Then run PyTest at the project root by typing
> `pytest` 