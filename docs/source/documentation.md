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
troubleshooting/index.md
citations/index.md
```

# OpenPoliceData

Welcome to the *new* OpenPoliceData documentation. The **OpenPoliceData (OPD)** Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 400+ incident-level datasets for about 4800 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. It provides a simple interface for finding publically available data and downloading data into [pandas](https://pandas.pydata.org/) DataFrames.

> **NEW IN VERSION 0.6**: OPD now provides tools for automated data standardization. Applying these tools allow you to start your analysis more quickly by replacing column names and data with standard values for some common column types. [Learn how it works and how to use it here.](getting_started/index.ipynb#Data-Standardization)

::::{grid} 3
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
:::{grid-item-card} {fas}`lightbulb;pst-color-primary` Examples
:link: examples/index.md
:link-alt: examples/index.md
Explore how to use OPD with Jupyter Notebooks 
:::
:::{grid-item-card} {fas}`code;pst-color-primary` Related Projects
:link: resources/index.md
:link-alt: resources/index.md
Find other police data projects
:::
:::{grid-item-card} {fas}`circle-question;pst-color-primary` Troubleshooting
:link: troubleshooting/index.md
:link-alt: troubleshooting/index.md
Search for help with common issues
:::
:::{grid-item-card} {fas}`bookmark;pst-color-primary` Citations
:link: citations/index.md
:link-alt: citations/index.md
Works citing OPD and how to cite OPD
:::
::::

*Only looking for a single dataset or want to explore the data available in OpenPoliceData?* Try out [OpenPoliceData Explorer](https://openpolicedata.streamlit.app/), our Streamlit web app!