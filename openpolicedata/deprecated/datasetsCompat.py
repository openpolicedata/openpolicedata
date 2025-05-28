from ..datasets import query as _query
from ._decorators import deprecated

# For back compatibility, preferred usage is datasets.query
@deprecated("opd.datasets_query is deprecated and will be removed in the next release (v1.0). "
        "Please use opd.datasets.query instead.")
def datasets_query(source_name=None, state=None, agency=None, table_type=None):
    return _query(source_name=source_name, state=state, agency=agency, table_type=table_type)