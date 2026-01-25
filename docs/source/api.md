# API Reference

This page lists the most commonly used entry points in the `openpolicedata` Python API.

## Main Package

```{eval-rst}
.. automodule:: openpolicedata
   :no-members:
   :no-index:
```

### Core Classes

```{eval-rst}
.. currentmodule:: openpolicedata

.. autosummary::
   :nosignatures:

   Source
```

### Enums and Standard Columns

#### TableType

```{eval-rst}
.. autoclass:: openpolicedata.TableType
   :inherited-members: False
   :no-index:
```

#### DataType

```{eval-rst}
.. autoclass:: openpolicedata.DataType
   :inherited-members: False
   :no-index:
```

#### Column

```{eval-rst}
.. autodata:: openpolicedata.Column
   :no-index:
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


## Data Module

`Source.load(...)` returns a `Table` object, which wraps the returned dataframe plus metadata and helper methods.

```{eval-rst}
.. currentmodule:: openpolicedata.data

.. autosummary::
   :nosignatures:

   Table
```


```{eval-rst}
.. automodule:: openpolicedata.data
   :members:
   :no-index:
```