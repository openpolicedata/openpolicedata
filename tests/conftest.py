import pytest
from test_utils import get_datasets, get_outage_datasets


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--csvfile", action="store", default=None, help="Replace default opd_source_table.csv with an updated one for testing"
    )
    parser.addoption(
        "--source", action="store", default=None, help="Only test datasets from this source"
    )
    parser.addoption(
        "--start", action="store", default=0, help="Start at this index when looping over datasets"
    )
    parser.addoption(
        "--skip", action="store", default=None, help="Comma-separated list of sources to skip"
    )
    parser.addoption(
        "--loghtml", action="store", default=0, help="0 (default) or 1 indicating if URL warnings/errors should be logged"
    )

    parser.addoption("--use-changed-rows", action="store_true", help="Run tests only on changed rows")
    parser.addoption("--outages", action="store_true", help="Run tests only on datasets in outages table")



def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


# Define fixtures for each command line option

@pytest.fixture(scope='session')
def source(request):
    return request.config.option.source

@pytest.fixture(scope='session')
def start_idx(request):
    return int(request.config.option.start)

@pytest.fixture(scope='session')
def skip(request):
    skip = request.config.option.skip
    if skip:
        skip = skip.split(",")
        skip = [x.strip() for x in skip]
    else:
        skip = []
    return skip


@pytest.fixture(scope='session')
def loghtml(request):
    return int(request.config.option.loghtml)


@pytest.fixture(scope='session')
def all_datasets(request):
    return get_datasets(request.config.option.csvfile)


@pytest.fixture(scope='session')
def use_changed_rows(request):
    return request.config.option.use_changed_rows

@pytest.fixture(scope='session')
def outages(request):
    return request.config.option.outages

@pytest.fixture(scope='session')
def datasets(request, all_datasets, use_changed_rows, outages):
    csvfile = request.config.option.csvfile
    ds = get_datasets(csvfile, use_changed_rows) if use_changed_rows else all_datasets
    return get_outage_datasets(ds) if outages else ds
