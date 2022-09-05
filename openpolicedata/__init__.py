from ._version import __version__
from .data import Source
from .datasets import query as _query

# For back compatibility, preferred usage is datasets.query
def datasets_query(source_name=None, state=None, agency=None, table_type=None):
    import warnings
    warnings.warn(
        "opd.datasets_query is deprecated and will be removed in a future version. Please "
        "opd.datasets.query instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return _query(source_name=source_name, state=state, agency=agency, table_type=table_type)