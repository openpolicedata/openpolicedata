import io
import logging
import os
import pytest

from openpolicedata import log

def revert(lgr, stream):
    log.revert_logger_verbosity(lgr)

    clear(stream)
    assert len(lgr.handlers)==1
    assert lgr.handlers[0].name == log.stream_handler_name
    assert lgr.handlers[0].level == logging.WARNING

    test_logger_default(lgr, stream)

def clear(stream):
    stream.truncate(0)
    stream.seek(0)
    assert len(stream.getvalue()) == 0

@pytest.fixture()
def log_stream():
    stream = io.StringIO()
    yield stream
    clear(stream)

@pytest.fixture()
def logger(log_stream):
    logger = log.get_logger()
    # Redirect handler output so that it can be checked
    logger.handlers[0].setStream(log_stream)

    yield logger
    for handler in logger.handlers:
        if handler.name != log.stream_handler_name:
            logger.removeHandler(handler)

@pytest.fixture()
def log_filename():
    filename = "test.log"
    if os.path.exists(filename):
        os.remove(filename)
    yield filename
    if os.path.exists(filename):
        os.remove(filename)

def test_get_logger(logger):
    assert logger.name == log.log_name
    assert len(logger.handlers)==1
    assert logger.handlers[0].name == log.stream_handler_name
    assert logger.handlers[0].level == logging.WARNING

def test_logger_default(logger, log_stream):    
    logger.info('TEST')
    assert len(log_stream.getvalue())==0


def test_update_logger_verbosity_True(logger, log_stream):
    log.update_logger_verbosity(logger, True)
    assert len(logger.handlers)==1
    assert logger.handlers[0].level == logging.INFO

    logger.debug("TEST DEBUG")
    assert len(log_stream.getvalue())==0

    logger.info('TEST')
    assert len(log_stream.getvalue()) > 0

    revert(logger, log_stream)


def test_update_logger_verbosity_debug(logger, log_stream):
    log.update_logger_verbosity(logger, logging.DEBUG)
    assert len(logger.handlers)==1
    assert logger.handlers[0].level == logging.DEBUG

    logger.debug("TEST DEBUG")
    assert len(log_stream.getvalue()) > 0

    revert(logger, log_stream)


def test_update_logger_verbosity_file(logger, log_stream, log_filename):
    assert not os.path.exists(log_filename)
    log.update_logger_verbosity(logger, log_filename)
    assert len(logger.handlers)==2
    assert logger.handlers[0].level == logging.WARNING
    assert logger.handlers[1].level == logging.INFO

    logger.debug("TEST DEBUG")
    if os.path.exists(log_filename):
        assert os.path.getsize(log_filename)==0

    logger.info("INFO")
    assert len(log_stream.getvalue())==0
    assert os.path.exists(log_filename)
    assert os.path.getsize(log_filename) > 3

    revert(logger, log_stream)


def test_temp_logging_change_false(logger, log_stream):
    with log.temp_logging_change(False):
        test_logger_default(logger, log_stream)

    test_get_logger(logger)
    test_logger_default(logger, log_stream)


def test_temp_logging_change_true(logger, log_stream):
    with log.temp_logging_change(True):
        assert len(logger.handlers)==1
        assert logger.handlers[0].level == logging.INFO

        logger.debug("TEST DEBUG")
        assert len(log_stream.getvalue())==0

        logger.info('TEST')
        assert len(log_stream.getvalue()) > 0

    clear(log_stream)
    test_get_logger(logger)
    test_logger_default(logger, log_stream)

@pytest.mark.parametrize('debug', ['DEBUG', logging.DEBUG])
def test_temp_logging_change_true_debug(logger, log_stream, debug):
    with log.temp_logging_change(True, debug):
        assert len(logger.handlers)==1
        assert logger.handlers[0].level == logging.DEBUG

        logger.debug("TEST DEBUG")
        assert len(log_stream.getvalue()) > 0

    clear(log_stream)
    test_get_logger(logger)
    test_logger_default(logger, log_stream)

def test_temp_logging_change_true_error(logger, log_stream):
    with pytest.raises(ValueError):
        with log.temp_logging_change(True):
            logger.info('TEST')
            assert len(log_stream.getvalue()) > 0
            raise ValueError('Test context manager')

    assert len(log_stream.getvalue()) > 0
    clear(log_stream)
    test_get_logger(logger)
    test_logger_default(logger, log_stream)