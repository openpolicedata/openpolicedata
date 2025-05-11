import pytest
import sys

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders


def test_process_date_input_empty():
    with pytest.raises(ValueError):
        data_loaders.data_loader._process_date([])
    
def test_process_date_too_many():
    year = [2021, 2022, 2023]
    with pytest.raises(ValueError):
        data_loaders.data_loader._process_date(year)

def test_process_dates_year_input_wrong_order():
    year = [2023, 2021]
    with pytest.raises(ValueError):
        data_loaders.data_loader._process_date(year)
