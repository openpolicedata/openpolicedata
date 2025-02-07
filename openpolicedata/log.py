import contextlib
import logging

log_name = "opd"
stream_handler_name = 'opd'
file_handler_name = 'opd-file'

@contextlib.contextmanager
def temp_logging_change(verbose, if_verbose_true_level='INFO'):
    logger = get_logger()
    try:
        update_logger_verbosity(logger, verbose, if_verbose_true_level)
        yield logger
    finally:
        revert_logger_verbosity(logger)

def check_level(level):
    try:
        logging._checkLevel(level)
        return True
    except:
        return False

def get_logger():
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.WARNING)
    
    for h in logger.handlers:
        if h.name==stream_handler_name:
            break
    else:
        sh = logging.StreamHandler()
        sh.name = stream_handler_name
        sh.setLevel(logging.WARNING)
        logger.addHandler(sh)

    return logger

def set_main_level(logger, level):
    # Change log level of main handler
    for h in logger.handlers:
        if h.name==stream_handler_name:
            h.last_level = h.level  # Store so that reversion is possible
            h.setLevel(level)

def revert_level(logger):
    for h in logger.handlers:
        if h.name==stream_handler_name:
            if hasattr(h, 'last_level'):
                h.setLevel(h.last_level)
                logger.setLevel(h.last_level)

def add_file_handler(logger, filename, level='INFO'):
    # verbose is a filename
    logger.setLevel(level)
    for handler in logger.handlers:
        if handler.name == file_handler_name:
            # Handler already exists
            handler.setLevel(level)
            break
    else:
        fh = logging.FileHandler(filename)
        fh.setLevel(level)
        fh.name = file_handler_name
        logger.addHandler(fh)

def rem_file_handler(logger):
    for handler in logger.handlers:
        if handler.name == file_handler_name:
            logger.removeHandler(handler)

def update_logger_verbosity(logger, verbose, if_verbose_true_level='INFO'):
    if isinstance(verbose,str):
        if check_level(verbose):
            logger.setLevel(verbose)
            set_main_level(logger, verbose)
        else:
            add_file_handler(logger, verbose)  # verbose will be a filename
            set_main_level(logger, logging.WARNING)
    elif isinstance(verbose, int) and not isinstance(verbose, bool):
        logger.setLevel(verbose)
        set_main_level(logger, verbose)
    elif verbose:
        logger.setLevel(if_verbose_true_level)
        set_main_level(logger, if_verbose_true_level)

def revert_logger_verbosity(logger):
    revert_level(logger)
    rem_file_handler(logger)