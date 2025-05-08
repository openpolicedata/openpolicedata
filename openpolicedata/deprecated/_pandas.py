import warnings
from pandas import DataFrame, Series
from pandas.core.indexing import _iLocIndexer, _LocIndexer

def _cast_if_necessary(result):
    if isinstance(result, Series) and result.name == 'TableType' and \
        result.str.contains("SUBJECT").any():
        result = DeprecationHandlerSeries(result)
    elif isinstance(result, DataFrame) and 'TableType' in result and \
        result["TableType"].str.contains("SUBJECT").any():
        result = DeprecationHandlerDataFrame(result)

    return result

class _iLocIndexerSub(_iLocIndexer):
    def __getitem__(self, key):
        result = super().__getitem__(key)

        if isinstance(key, tuple) and len(key)>1:
            try:
                # 8 is where the Year column has been moved to and therefore, any old iloc calls
                # requesting data past index 8 could now return the wrong value
                if (isinstance(key[1], int) and 8<=key[1]<13) or \
                    (isinstance(key[1], slice) and any([8<=x<13 for x in range(*key[1].indices(1000000))])) or \
                    (not isinstance(key[1], (int, slice)) and any(8<=x<13 for x in key[1])):
                    warnings.warn(DeprecationWarning("The Year column of the datasets table has recently been moved to column 8 from column 12. "+\
                                                     f"Column input {key[1]} could be outputting a different column than desired for index 8-12 as a result "+\
                                                     "if the code was written prior to this change. Code may need to be changed. It is recommended "+\
                                                     "that .loc be used to index the datasets table rather than .iloc."))
            except:
                raise

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