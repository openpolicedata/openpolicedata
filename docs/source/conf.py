# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'OpenPoliceData'
copyright = 'OpenPoliceData contributors'
author = 'Matt Sowd and Paul Otto'
release = '0.5.4'

import os
import sys
docs_loc = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(docs_loc))
import openpolicedata

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# https://www.sphinx-doc.org/en/master/usage/extensions/index.html
# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.duration',
    'sphinx.ext.autosummary',
    'myst_parser',
    'nbsphinx',
    "numpydoc",
    'sphinx_design',
]

autosummary_generate = True
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autodoc_typehints
autodoc_typehints = "none"

myst_enable_extensions = ["colon_fence"]

# https://numpydoc.readthedocs.io/en/latest/install.html
numpydoc_attributes_as_param_list = False

templates_path = ['_templates']
exclude_patterns = []

# https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/layout.html

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
html_css_files = ["css/custom.css"]

# Unable to get this to work as a relative links so using full link for now
pypi_logo = "_static/pypi.svg"#os.path.join(docs_loc, "_static","pypi.svg")

# https://icon-sets.iconify.design/simple-icons/streamlit/
streamlit_logo = "_static/streamlit.svg"#os.path.join(docs_loc, "_static","pypi.svg")

# Define the json_url for our version switcher.
json_url = "https://openpolicedata.readthedocs.io/en/latest/_static/switcher.json"

# https://github.com/pydata/pydata-sphinx-theme/blob/main/docs/conf.py
# Define the version we use for matching in the version switcher.
version_match = os.environ.get("READTHEDOCS_VERSION")
# If READTHEDOCS_VERSION doesn't exist, we're not on RTD
# If it is an integer, we're in a PR build and the version isn't correct.
# If it's "latest" → change to "dev" (that's what we want the switcher to call it)
# We want to keep the relative reference if we are in dev mode
# but we want the whole url if we are effectively in a released version
json_url = "_static/switcher.json" if version_match is None else json_url
if not version_match or version_match.isdigit() or version_match == "latest":
    # For local development, infer the version to match from the package.
    release = openpolicedata.__version__
    if "dev" in release or "rc" in release:
        version_match = "dev"
    else:
        version_match = "v" + release

html_theme_options = {
    "logo": {
        "text": "OpenPoliceData",
        "alt_text": "OpenPoliceData",
    },
    "github_url": "https://github.com/openpolicedata/openpolicedata",
    "icon_links": [
        {
            "name": "PyPI",
            # URL where the link will redirect
            "url": "https://pypi.org/project/openpolicedata/",  # required
            # Icon class (if "type": "fontawesome"), or path to local image (if "type": "local")
            "icon": pypi_logo,
            # The type of image to be used (see below for details)
            "type": "local",
        },
        {
            "name": "Streamlit",
            # URL where the link will redirect
            "url": "https://openpolicedata.streamlit.app/",  # required
            # Icon class (if "type": "fontawesome"), or path to local image (if "type": "local")
            "icon": streamlit_logo,
            # The type of image to be used (see below for details)
            "type": "local",
        }
   ],
    # "navbar_end": ["version-switcher", "theme-switcher", "navbar-icon-links"],
    "switcher": {
        "json_url": json_url,
        "version_match": version_match,
    },
    "navbar_center": ["version-switcher", "navbar-nav"],
}

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

source_parsers = {'.md': 'recommonmark.parser.CommonMarkParser'}

# This removes execution counts when displaying Jupyter notebooks
# https://nbsphinx.readthedocs.io/en/0.8.9/custom-css.html
nbsphinx_prolog = """
.. raw:: html

    <style>
        .nbinput .prompt,
        .nboutput .prompt {
            display: none;
        }
    </style>
"""

nbsphinx_execute = 'never'