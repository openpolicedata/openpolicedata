__all__ = ["data", "data_loaders", "datasets"]
import sys
import os

print(f"The __init__.py system path is {sys.path} and current directory is {os.getcwd()}")
from . import data
from . import data_loaders
from . import datasets
