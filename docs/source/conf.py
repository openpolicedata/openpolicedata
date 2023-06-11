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
    "sphinx.ext.intersphinx",
    'sphinx.ext.autosummary',
    "numpydoc",
]

autosummary_generate = True
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autodoc_typehints
autodoc_typehints = "none"

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

# Unable to get this to work as a relative links so using full link for now
pypi_logo = os.path.join(docs_loc, "_static","pypi.svg")

html_theme_options = {
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
        }
   ]
    # "navbar_end": ["version-switcher", "theme-switcher", "navbar-icon-links"],
}

intersphinx_mapping = {
        "pandas": ("https://pandas.pydata.org/", None),
        "python": ("https://docs.python.org/3/", None),
    }

# TODO: https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/version-dropdown.html