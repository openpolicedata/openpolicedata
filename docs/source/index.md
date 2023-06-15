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

getting_started/index
datasets/index
examples/index
api/index
resources/index
```

# OpenPoliceData

**OpenPoliceData (OPD)** is a Python library providing easy access to 365+ incident-level police datasets including traffic stops, use of force, officer-involved shootings, and complaints data. It provides a simple interface for finding publically available data and downloading data into [pandas](https://pandas.pydata.org/) DataFrames.

::::{grid} 3
:gutter: 1 2 3 4

:::{grid-item-card} {fas}`star;pst-color-primary` Getting Started
:link: getting_started/index
:link-type: doc
:link-alt: getting_started/index
Find out how to install OPD and learn the basics
:::
:::{grid-item-card} {fas}`database;pst-color-primary` Datasets
:link: datasets/index
:link-type: doc
:link-alt: datasets/index
Learn what types of data are available in OPD and for what agencies
:::
:::{grid-item-card} {fas}`lightbulb;pst-color-primary` Examples
:link: examples/index
:link-type: doc
:link-alt: examples/index
Explore how to use OPD with Jupyter Notebooks 
:::
:::{grid-item-card} {fas}`book;pst-color-primary` API Reference
:link: api/index
:link-type: doc
:link-alt: api/index
Detailed descriptions of functions and classes
:::
:::{grid-item-card} {fas}`bookmark;pst-color-primary` Other Resources
:link: resources/index
:link-type: doc
:link-alt: resources/index
Other resources for finding police data
:::
::::