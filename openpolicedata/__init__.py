import sys
import os
print(f"The __init__.py system path is {sys.path} and current directory is {os.getcwd()}")

#For all to work we need a path into /home/user/cjc/openpolicedata/openpolicedata
__all__ = [ "data", "data_loaders", "datasets"]



#These
from openpolicedata.data_loaders import *
from openpolicedata.datasets import *
from openpolicedata.data import *
#from openpolicedata import data_loaders
#from openpolicedata import datasets
