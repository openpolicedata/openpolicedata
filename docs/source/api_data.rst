Data Access
===========

The data access API centers on two classes: ``Source`` for discovery and loading, and ``Table`` for working with loaded results.

Source
------

.. currentmodule:: openpolicedata

.. autosummary::
   :toctree: generated/
   :nosignatures:
   :template: autosummary/class_without_autosummary.rst

   Source

Source Methods
--------------

.. autosummary::
   :toctree: generated/

   Source.get_tables_types
   Source.get_years
   Source.get_agencies
   Source.get_count
   Source.load
   Source.load_iter
   Source.filter
   Source.find_related_tables
   Source.load_csv
   Source.load_feather
   Source.load_parquet

Table
-----

.. currentmodule:: openpolicedata.data

.. autosummary::
   :toctree: generated/
   :nosignatures:
   :template: autosummary/class_without_autosummary.rst

   Table

Table Methods
-------------

.. autosummary::
   :toctree: generated/

   Table.expand
   Table.merge
   Table.standardize
   Table.get_transform_map
   Table.get_race_col
   Table.get_gender_col
   Table.get_age_col
   Table.to_csv
   Table.to_feather
   Table.to_parquet
   Table.get_csv_filename
   Table.get_feather_filename
   Table.get_parquet_filename

Module Contents
---------------

.. automodule:: openpolicedata.data
   :no-members: