from pandas import DataFrame, Series
from pandas.core.indexing import _iLocIndexer, _LocIndexer

def _cast_if_necessary(result):
    if isinstance(result, Series) and result.name == 'TableType' and \
        result.str.contains("SUBJECT").any():
        result = DeprecationHandlerSeries(result)
    elif isinstance(result, DataFrame) and len(result)>0 and 'TableType' in result and \
        result["TableType"].str.contains("SUBJECT").any():
        result = DeprecationHandlerDataFrame(result)

    return result

class _iLocIndexerSub(_iLocIndexer):
    def __getitem__(self, key):
        result = super().__getitem__(key)

        return _cast_if_necessary(result)
 

class _LocIndexerSub(_LocIndexer):
    def __getitem__(self, key):
        result = super().__getitem__(key)
        
        return _cast_if_necessary(result)
 

class DeprecationHandlerSeries(Series):
    def _cmp_method(self, other, op):
        return super()._cmp_method(other, op)
 

    def isin(self, values) -> Series:
        return super().isin(values)       


class DeprecationHandlerDataFrame(DataFrame):
    def __getitem__(self, key):
        result = super().__getitem__(key)
        return _cast_if_necessary(result)
    

    def query(self, *args, **kwargs):
        result = super().query(*args, **kwargs)
        return _cast_if_necessary(result)
    

    def copy(self, deep = True):
        result = super().copy(deep)
        return DeprecationHandlerDataFrame(result)

    @property
    def iloc(self) -> _iLocIndexerSub:
        return _iLocIndexerSub("iloc", self)

    @property
    def loc(self) -> _LocIndexerSub:
        return _LocIndexerSub("loc", self)