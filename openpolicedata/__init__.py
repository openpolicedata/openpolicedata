"""
OpenPoliceData (OPD): A Python package for accessing and analyzing police data.

OpenPoliceData provides tools to streamline access to incident-level police data in the 
United States, including traffic stops, use of force incidents, officer-involved 
shootings, and complaints. The package enables users to query and download data from
various law enforcement agencies through a consistent interface.

Key Components:
---------------
Source : class
    Main class for accessing and querying data sources.
TableType : enum
    Enumeration of available table types (e.g., stops, use_of_force).
Column : module
    Standard column definitions for normalizing data access.
datasets : module
    Access to the catalog of available datasets.

Examples:
---------
>>> import openpolicedata as opd
>>> source = opd.Source("AGENCY_NAME")
>>> data = source.load("stops", year=2022)

See Also:
---------
Documentation: https://openpolicedata.readthedocs.io/
GitHub: https://github.com/openpolicedata/openpolicedata
"""

from ._version import __version__
from .data import Source
from . import defs
from . import datasets
from .defs import TableType
from .defs import DataType
from .defs import columns as Column