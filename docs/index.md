---
myst:
  html_meta:
    "description lang=en": |
      Top-level documentation for OpenPoliceData.
html_theme.sidebar_secondary.remove: true
---

```{toctree}
:maxdepth: 2
:hidden:

source/getting_started/index.ipynb
source/datasets/index.ipynb
source/examples/index.ipynb
```

# OpenPoliceData

Welcome to the *BETA* version of the OpenPoliceData documentation. **OpenPoliceData (OPD)** is a Python library providing easy access to 365+ incident-level police datasets including traffic stops, use of force, officer-involved shootings, and complaints data. It provides a simple interface for finding publically available data and downloading data into [pandas](https://pandas.pydata.org/) DataFrames.

::::{grid} 3
:gutter: 1 2 3 4

:::{grid-item-card} {fas}`star;pst-color-primary` Getting Started
:link: source/getting_started/index.ipynb
:link-alt: source/getting_started/index.ipynb
Find out how to install OPD and learn the basics
:::
:::{grid-item-card} {fas}`database;pst-color-primary` Datasets
:link: source/datasets/index.ipynb
:link-alt: source/datasets/index.ipynb
Learn what types of data are available in OPD and for what agencies
:::
:::{grid-item-card} {fas}`lightbulb;pst-color-primary` Examples
:link: source/examples/index.ipynb
:link-alt: source/examples/index.ipynb
Explore how to use OPD with Jupyter Notebooks 
:::
::::

*Only looking for a single dataset or want to explore the data available in OpenPoliceData?* Try out [OpenPoliceData Explorer](https://openpolicedata.streamlit.app/), our Streamlit web app!*