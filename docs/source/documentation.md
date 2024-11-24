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

> **OPD User Survey**: We would love to hear how you are using OpenPoliceData and how we can make it better! [Please take a moment to fill out this short survey.](https://docs.google.com/forms/d/e/1FAIpQLScvhcKQwvPmUK6wV0YKQipGsTsz0uzyVdT8FQsQ5g2RBvNh0g/viewform?usp=pp_url)

> Older OpenPoliceData versions (<0.6) must be upgraded. Data source table format changes that were not backward-compatible were introduced to provide access to more datasets. However, a methodology for providing older source table formats for older OPD versions was not introduced until version 0.6. Run `pip install openpolicedata --upgrade` from the command line to upgrade OPD.

# OpenPoliceData

Welcome to the *new* OpenPoliceData documentation. The **OpenPoliceData (OPD)** Python library is the most comprehensive centralized public access point for incident-level police data in the United States. OPD provides easy access to 500+ incident-level datasets for about 4865 police agencies. Types of data include traffic stops, use of force, officer-involved shootings, and complaints. It provides a simple interface for finding publically available data and downloading data into [pandas](https://pandas.pydata.org/) DataFrames.

<!-- https://fontawesome.com/icons?d=gallery&amp%3Bm=free -->

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

### Advanced Topics
The follow guides describe more advanced topics that may interest you after viewing the [Getting Started Guide](./getting_started/index.ipynb).

::::{grid} 2
:gutter: 1 2 3 4

:::{grid-item-card} {fas}`calendar-days;pst-color-primary` Year/Date Filtering
:link: getting_started/year_filtering.ipynb
:link-alt: getting_started/year_filtering.ipynb
More advanced year/date filtering and how to handle special (rare) cases
:::
:::{grid-item-card} {fas}`broom;pst-color-primary` Data Standardization
:link: examples/opd-examples/standardization.ipynb
:link-alt: examples/opd-examples/standardization.ipynb
How to customize (optional) data standardization processing
:::
::::

*Only looking for a single dataset or want to explore the data available in OpenPoliceData?* Try out [OpenPoliceData Explorer](https://openpolicedata.streamlit.app/), our Streamlit web app!