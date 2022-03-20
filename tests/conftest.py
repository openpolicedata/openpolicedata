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