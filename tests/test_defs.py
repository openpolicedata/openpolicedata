if __name__ == "__main__":
	import sys
	sys.path.append('../openpolicedata')
	
from openpolicedata.defs import TableType

def test_enum_str_class():
	assert str(TableType.ARRESTS) == TableType.ARRESTS.value
	

def test_enum_value_equality():
	assert TableType.CALLS_FOR_SERVICE == TableType.CALLS_FOR_SERVICE.value