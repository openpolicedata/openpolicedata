import sys

import pytest

if __name__ == "__main__":
	sys.path.append('../openpolicedata')
from openpolicedata import data_loaders

from test_utils import check_for_dataset


def test_date_range_request_on_year_date_field():
    source = 'State Police'
    state = 'New Jersey'
    table = 'TRAFFIC STOPS'
    url = 'data.nj.gov'
    data_set = 'kie7-5sud'
    date_field = 'year'
    if not check_for_dataset(source, table, state):
        return
    
    loader = data_loaders.Socrata(url=url, data_set=data_set, date_field=date_field)
    date = ['2021-11-01', '2021-12-01']
    with pytest.raises(ValueError, match='Unable to filter by date. Filter by year instead. Date column only provides the year.'):
        loader.load(date=date)


def test_yyyymmdd_test():
    raise NotImplementedError()