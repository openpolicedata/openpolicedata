# openpolicedata
A Python package for accessing police traffic stop, arrests, use of force, etc. data

## Setup
 
* After cloning the repo with 

> `git clone git@github.com:sowdm/openpolicedata.git`
* Setup and activate the virtual environment

> `python3 -m venv .venv`

> `source .venv/bin/activate`
* Then install the dependencies

> `pip install -r requirements.txt`
* And optionally install Jupyter Notebooks

> `pip install jupyterlab`
* And optionally install PyTest

> `pip install pytest`


To run the tests see the *[readme](./tests/README.md)* in the testing directory.


### Using VSCode
If using VSCode the path to the src directory can be added to the Python path by creating a file called `.env` in the root directory. This file should include the line: `PYTHONPATH=./src`

