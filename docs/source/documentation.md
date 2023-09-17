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

getting_started/index.ipynb
datasets/index.ipynb
examples/index.md
resources/index.md
```

# OpenPoliceData

Welcome to the *new* OpenPoliceData documentation. The **OpenPoliceData (OPD)** Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 383+ incident-level datasets for over 3500 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. It provides a simple interface for finding publically available data and downloading data into [pandas](https://pandas.pydata.org/) DataFrames.

::::{grid} 2
:gutter: 1 2 3 4

:::{grid-item-card} {fas}`star;pst-color-primary` Getting Started
:link: getting_started/index.ipynb
:link-alt: getting_started/index.ipynb
Find out how to install OPD and learn the basics
:::
:::{grid-item-card} {fas}`database;pst-color-primary` Datasets
:link: datasets/index.ipynb
:link-alt: datasets/index.ipynb
Learn what types of data are available in OPD and for what agencies
:::
::::

::::{grid} 2
:gutter: 1 2 3 4

:::{grid-item-card} {fas}`lightbulb;pst-color-primary` Examples
:link: examples/index.md
:link-alt: examples/index.md
Explore how to use OPD with Jupyter Notebooks 
:::
:::{grid-item-card} {fas}`bookmark;pst-color-primary` Related Projects
:link: resources/index.md
:link-alt: resources/index.md
Find other police data projects
:::
::::

*Only looking for a single dataset or want to explore the data available in OpenPoliceData?* Try out [OpenPoliceData Explorer](https://openpolicedata.streamlit.app/), our Streamlit web app!