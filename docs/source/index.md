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

documentation.md
api.md
getting_started/index.ipynb
datasets/index.ipynb
examples/index.md
resources/index.md
troubleshooting/index.md
citations/index.md
modules
```

> **OPD User Survey**: We would love to hear how you are using OpenPoliceData and how we can make it better! [Please take a moment to fill out this short survey.](https://docs.google.com/forms/d/e/1FAIpQLScvhcKQwvPmUK6wV0YKQipGsTsz0uzyVdT8FQsQ5g2RBvNh0g/viewform?usp=pp_url)

# OpenPoliceData

**OpenPoliceData (OPD)** aims to make it easier to access and analyze police data. OPD's police data download tools provide the most comprehensive centralized public access to incident-level police data in the United States. Currently, our tools enable easy access to 550+ incident-level datasets from about 236 police agencies and 11 entire states. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. All data is sourced either directly from police departments or state agencies or through the [Stanford Open Policing Project](https://openpolicing.stanford.edu/). When data is loaded by OPD, the returned data is unmodified (with the exception of formatting known date fields) from what appears on the source's site, and OPD provides links to the original data for transparency. In the future, we hope to expand our offerings to include data dashboards that allow users to do basic analysis of OPD's datasets using web apps.

Here are the current tools provided by OpenPoliceData:

::::{grid} 2
<!-- :gutter: 1 2 3 4 -->

:::{grid-item-card} {fas}`star;pst-color-primary` OpenPoliceData Python Library
:link: documentation.md
:link-alt: ref
Documentation for the `openpolicedata` Python API (data is returned as pandas DataFrames).
:::
:::{grid-item-card} {fas}`database;pst-color-primary` OpenPoliceData Explorer
:link: https://openpolicedata.streamlit.app/
Explore available data and download individual OPD datasets using our Streamlit app.
:::
::::