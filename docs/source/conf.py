# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'OpenPoliceData'
copyright = 'OpenPoliceData contributors'
author = 'Matt Sowd and Paul Otto'

import os
import sys
docs_loc = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(docs_loc))
import openpolicedata

release = getattr(openpolicedata, "__version__", "")

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# https://www.sphinx-doc.org/en/master/usage/extensions/index.html
# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.doctest',
    'sphinx.ext.duration',
    'sphinx.ext.autosummary', # Generate autodoc summaries
    'myst_parser',
    'nbsphinx',
    "numpydoc", # for NumPy/Google style docstrings https://numpydoc.readthedocs.io/en/latest/
    'sphinx_design',
]

autosummary_generate = True
autosummary_mock_imports = []
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autodoc_typehints
autodoc_typehints = "none"
autodoc_inherit_docstrings = False
autodoc_default_options = {
    'members': True,
    'inherited-members': False, # Set to True if you want to include inherited members from parent classes
    'undoc-members': False,       # Include members even if they don't have a docstring (optional)
    'private-members': False,      # Change to True if you want to document private members too
    'show-inheritance': False,
    
}

# Add this near your other autosummary settings
numpydoc_show_class_members = False  # This prevents numpydoc from auto-generating autosummary tables

# Alternatively, you can keep the autosummary tables but tell it to ignore inherited members
# numpydoc_class_members_toctree = False

# Add this to explicitly exclude certain classes from having autosummary tables generated
numpydoc_show_inherited_class_members = {
    'openpolicedata.DataType': False,
    'openpolicedata.defs.DataType': False,
    'openpolicedata.TableType': False,
    'openpolicedata.defs.TableType': False,
}

STR_METHODS_TO_SKIP = {
    'capitalize', 'casefold', 'center', 'count', 'encode', 'endswith',
    'expandtabs', 'find', 'format', 'format_map', 'index', 'isalnum',
    'isalpha', 'isascii', 'isdecimal', 'isdigit', 'isidentifier',
    'islower', 'isnumeric', 'isprintable', 'isspace', 'istitle',
    'isupper', 'join', 'ljust', 'lower', 'lstrip', 'maketrans',
    'partition', 'removeprefix', 'removesuffix', 'replace', 'rfind',
    'rindex', 'rjust', 'rpartition', 'rsplit', 'rstrip', 'split',
    'splitlines', 'startswith', 'strip', 'swapcase', 'title',
    'translate', 'upper', 'zfill'
}

def autodoc_skip_str_methods_for_enums(app, what, name, obj, skip, options):
    if what == "method" and name in STR_METHODS_TO_SKIP:
        # Only skip for DataType and TableType
        if hasattr(obj, '__qualname__') and obj.__qualname__.startswith(("DataType.", "TableType.")):
            return True
    return skip


def skip_enum_value_members(app, what, name, obj, skip, options):
    """
    Hide individual Enum value members for DataType and TableType so only the
    class docstring is shown.
    """
    try:
        from openpolicedata import defs as _defs
        if isinstance(obj, (_defs.DataType, _defs.TableType)):
            return True
    except Exception:
        pass
    return skip

#def setup(app):
def setup(app):
    app.connect('autodoc-skip-member', skip_enum_value_members)
    app.connect('autodoc-skip-member', autodoc_skip_str_methods_for_enums)
    return {'parallel_read_safe': True, 'parallel_write_safe': True}

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
version_match = 'dev' if not version_match or version_match=='latest' else version_match

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
#    "show_version_warning_banner": True,
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