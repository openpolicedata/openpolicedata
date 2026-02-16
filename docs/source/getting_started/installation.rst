.. _getting_started.installation:


Advanced Installation
=====================

OpenPoliceData is installed from `PyPI <https://pypi.org/project/openpolicedata/>`__.

Basic installation:
-------------------
The standard installation will enable almost all datasets available in OpenPoliceData to be loaded.

.. code-block:: bash

    pip install openpolicedata


Install GeoPandas
----------------------------------
If `GeoPandas <https://geopandas.org/en/stable/>`__ is installed, OpenPoliceData can load data that contains location information into `GeoDataFrames <https://geopandas.org/en/stable/docs/reference/geodataframe.html>`__ to aid geospatial analysis. Data without location information will still be loaded as a `pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`__. The easiest way to `install GeoPandas <https://geopandas.org/en/stable/getting_started/install.html>`__ is with conda:

.. code-block:: bash

    conda install geopandas


Install with Optional Dependencies
----------------------------------
A few datasets require the installation of additional packages (`rapidfuzz <https://pypi.org/project/rapidfuzz/>`__ and `msoffcrypto-tool <https://pypi.org/project/msoffcrypto-tool/>`__) in order to load them. They can be installed with OpenPoliceData with the following pip command:

.. code-block:: bash

    pip install "openpolicedata[optional]"

Alternatively, the user can wait to install these packages if they are ever needed. OpenPoliceData throws an error with the required optional packages when a dataset is read that requires them.