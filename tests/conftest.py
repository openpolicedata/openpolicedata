# content of conftest.py
# https://docs.pytest.org/en/latest/example/simple.html
import pytest


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
        "--last", action="store", default=None, help="Run only the last N datasets in tests"
    )
    parser.addoption(
        "--skip", action="store", default=None, help="Comma-separated list of sources to skip"
    )
    parser.addoption(
        "--loghtml", action="store", default=0, help="0 (default) or 1 indicating if URL warnings/errors should be logged"
    )


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


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.csvfile
    if 'csvfile' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("csvfile", [option_value])
    else:
        metafunc.parametrize("csvfile", [None])

    option_value = metafunc.config.option.source
    if 'source' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("source", [option_value])
    else:
        metafunc.parametrize("source", [None])

    option_value = metafunc.config.option.last
    if 'last' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("last", [int(option_value)])
    else:
        metafunc.parametrize("last", [float('inf')])

    option_value = metafunc.config.option.skip
    if 'skip' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("skip", [option_value])
    else:
        metafunc.parametrize("skip", [None])

    option_value = metafunc.config.option.loghtml
    metafunc.parametrize("loghtml", [option_value])