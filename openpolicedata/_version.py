import os
__version__ = "0.5.7"

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '_version.txt')) as _f:
    __version__ = _f.readline().strip()