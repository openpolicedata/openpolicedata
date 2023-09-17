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

**OpenPoliceData (OPD)** aims to make it easier to access and analyze police data. OPD's police data download tools provide the most comprehensive centralized public access to incident-level police data in the United States. Currently, our tools enable easy access to 383+ incident-level datasets for over 3500 police agencies including traffic stops, use of force, officer-involved shootings, and complaints data. All data is sourced either directly from police departments or state agencies or through the [Stanford Open Policing Project](https://openpolicing.stanford.edu/)  In the future, we hope to expand our offerings to include data dashboards that allow users to do basic analysis of OPD's datasets using web apps.

Here are the current tools provided by OpenPoliceData:

::::{grid} 2
<!-- :gutter: 1 2 3 4 -->

:::{grid-item-card} {fas}`star;pst-color-primary` OpenPoliceData Python Library
:link: documentation.md
:link-alt: ref
Access OPD's datasets using simple Python commands. Data is returned as pandas DataFrames.
:::
:::{grid-item-card} {fas}`database;pst-color-primary` OpenPoliceData Explorer
:link: https://openpolicedata.streamlit.app/
Explore available data and download individual OPD datasets using our Streamlit app.
:::
::::