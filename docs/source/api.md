# API Reference

This page lists the most commonly used entry points in the `openpolicedata` Python API.

## Main Package

```{eval-rst}
.. automodule:: openpolicedata
   :no-members:
   :noindex:
```

### Core Classes

{py:class}`openpolicedata.Source` explores a data source and loads its data.
Create a source with `opd.Source(source_name, state=None, agency=None)`, then
use its discovery and loading methods described in [Data Access](#data-access-opd-data).

### Enums and Standard Columns

#### TableType

```{eval-rst}
.. autoclass:: openpolicedata.TableType
   :no-members:
   :no-inherited-members:
   :noindex:
```

#### DataType

```{eval-rst}
.. autoclass:: openpolicedata.DataType
   :no-members:
   :no-inherited-members:
   :noindex:
```

#### Column

```{eval-rst}
.. autodata:: openpolicedata.Column
   :no-value:
   :noindex:

   Standard column names used by ``Table.standardize()``. ``Column`` is an
   object containing string constants, accessed as ``opd.Column.DATE`` or
   ``opd.Column.RACE_SUBJECT``.

   Attribute names describe the field and role; their values are the actual
   dataframe column names. For example, ``opd.Column.RACE_SUBJECT`` is
   ``"SUBJECT_RACE"`` and ``opd.Column.AGE_OFFICER`` is ``"OFFICER_AGE"``.

   Use ``opd.Column.to_dict()`` for the complete mapping of attributes to
   column names, or display ``opd.Column`` in a notebook to see all names
   and their definitions. Available columns depend on the source data.
```


## Dataset Catalog (`opd.datasets`)

```{eval-rst}
.. currentmodule:: openpolicedata.datasets

.. autosummary::
   :nosignatures:

   query
   reload
   num_unique
   num_sources
   summary_by_state
   summary_by_table_type
   get_table_types
```


## Dataset IDs (`opd.dataset_id`)

```{eval-rst}
.. currentmodule:: openpolicedata.dataset_id

.. autosummary::
   :nosignatures:

   parse
   expand
   is_combined_dataset
   parse_excel_dataset
```


## Standardization (`opd.preproc`)

```{eval-rst}
.. currentmodule:: openpolicedata.preproc

.. autosummary::
   :nosignatures:

   standardize
```


## Exceptions

```{eval-rst}
.. currentmodule:: openpolicedata.exceptions

.. autosummary::
   :nosignatures:

   OPD_GeneralError
   OPD_DataUnavailableError
   OPD_TooManyRequestsError
   OPD_SocrataHTTPError
   OPD_MinVersionError
   CompatSourceTableLoadError
```


## Data Access (`opd.data`)

The data access API centers on two classes:

- `opd.Source` selects entries from the dataset catalog and loads their data.
- `opd.data.Table` combines a loaded pandas or GeoPandas dataframe with its
  source metadata and tools for standardizing, combining, and exporting data.

Create a `Source` directly. Its `load` and `load_iter` methods return `Table`
objects; the underlying dataframe is available through `Table.table`. Direct
construction of a `Table` is normally unnecessary.

`Table.standardize` and `Table.expand` modify a table in place, while
`Table.merge` returns a new `Table`. Export methods return the path of the file
they create.

### Source

`Source` is also exported from the main package as `opd.Source`. Its `datasets`
attribute contains the catalog rows matching the source name, state, and agency
supplied to the constructor.

```{eval-rst}
.. currentmodule:: openpolicedata

.. autosummary::
   :nosignatures:

   Source
```

#### Discovery and Dataset Selection

Use these methods to inspect a source's available data and narrow requests that
could match more than one catalog entry.

```{eval-rst}
.. currentmodule:: openpolicedata

.. autosummary::

   Source.get_tables_types
   Source.get_years
   Source.get_agencies
   Source.filter
   Source.check_simple_dataset_filter
   Source.find_related_tables
```

#### Loading Data

`load` retrieves a complete result, `load_iter` yields batches, and `get_count`
reports the number of matching records. The file-loading methods reconstruct a
`Table` from data previously saved by its corresponding export method.

```{eval-rst}
.. currentmodule:: openpolicedata

.. autosummary::

   Source.get_count
   Source.load
   Source.load_iter
   Source.load_csv
   Source.load_feather
   Source.load_parquet
```

#### Default Filenames

These methods calculate the same metadata-based filenames used when saving and
reloading tables. `output_dir` can be used to include a destination directory in
the returned path.

```{eval-rst}
.. currentmodule:: openpolicedata

.. autosummary::

   Source.get_csv_filename
   Source.get_feather_filename
   Source.get_parquet_filename
```

### Table

A `Table` stores the dataframe in `table` and exposes source information such as
`state`, `source_name`, `agency`, `table_type`, `date`, and `urls`. The `is_std`
attribute indicates whether `standardize` has been applied.

```{eval-rst}
.. currentmodule:: openpolicedata.data

.. autosummary::
   :nosignatures:

   Table
```

#### Standardizing and Combining Data

After standardization, the demographic column helpers return the standardized
column name for the requested subject or officer role. `get_transform_map`
describes the column and value transformations that were applied.

```{eval-rst}
.. currentmodule:: openpolicedata.data

.. autosummary::

   Table.standardize
   Table.expand
   Table.merge
   Table.get_transform_map
   Table.get_race_col
   Table.get_gender_col
   Table.get_age_col
```

#### Exporting Data

```{eval-rst}
.. currentmodule:: openpolicedata.data

.. autosummary::

   Table.to_csv
   Table.to_feather
   Table.to_parquet
   Table.get_csv_filename
   Table.get_feather_filename
   Table.get_parquet_filename
```

### Module-Level Filename Helpers

Use these functions when metadata is available but no `Source` or `Table` object
has been created.

```{eval-rst}
.. currentmodule:: openpolicedata.data

.. autosummary::

   get_csv_filename
   get_feather_filename
   get_parquet_filename
```
