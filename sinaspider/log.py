"""
A logger initializer.
"""

import datetime
import logging
import logging.handlers
import os

from sinaspider.config import *

# Private members


def _getLevel(name):
    return logging._nameToLevel.get(name, 'INFO')


_COLOR_RESET = '\033[1;0m'
_COLOR_RED = '\033[1;31m'
_COLOR_GREEN = '\033[1;32m'
_COLOR_YELLOW = '\033[1;33m'
_COLOR_BLUE = '\033[1;34m'
_COLORERD_LOG_FMAT = {
    'DEBUG': _COLOR_BLUE + '%s' + _COLOR_RESET,
    'INFO': _COLOR_GREEN + '%s' + _COLOR_RESET,
    'WARNING': _COLOR_YELLOW + '%s' + _COLOR_RESET,
    'ERROR': _COLOR_RED + '%s' + _COLOR_RESET,
    'CRITICAL': _COLOR_RED + '%s' + _COLOR_RESET,
}


class ColoredFormatter(logging.Formatter):
    """
    A colorful formatter.
    """

    def __init__(self, fmt=None, datefmt=None):
        logging.Formatter.__init__(self, fmt, datefmt)

    def format(self, record):
        """
        Return a colorful output in console
        """
        level_name = record.levelname
        msg = logging.Formatter.format(self, record)
        return _COLORERD_LOG_FMAT.get(level_name, '%s') % msg

def _configure_logger(name):
    """
    Configure the logger.
    """
    now = datetime.datetime.now().strftime("%y-%m-%d")
    fmt = LOGGER_CONFIG['format']
    datefmt = LOGGER_CONFIG['datefmt']
    formatter = logging.Formatter(fmt, datefmt)
    stream_formatter = ColoredFormatter(fmt, datefmt)
    logger = logging.getLogger()
    dir_path = os.path.dirname(os.path.abspath(__file__))
    dir_path = os.path.join(os.path.dirname(dir_path), 'log')
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    if LOGGER_CONFIG['use_terminal']:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)
    name = now + name
    handler = logging.handlers.TimedRotatingFileHandler(name, when='D',
                                                        backupCount=7)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def configure_listener_logger(log_path):
    """
    Initialize logging facility.

    Input:
    - log_path: A string of absolute log file path.
    - when: A single character of 'S', 'M', 'H', 'D', 'W' to indicate how to split 
            the log file by time interval.

            'S' : Seconds
            'M' : Minutes
            'H' : Hours
            'D' : Days
            'W' : Week day
            default value: 'D'

    Returns True if initialize the logger successfully.
    """
    fmt = LOGGER_CONFIG['format']
    datefmt = LOGGER_CONFIG['datefmt']
    formatter = logging.Formatter(fmt, datefmt)
    stream_formatter = ColoredFormatter(fmt, datefmt)
    logger = logging.getLogger()
    dir_path = os.path.dirname(log_path)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    if LOGGER_CONFIG['use_terminal']:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)
    handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log", when='D',
                                                        backupCount=7)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def configure_logger(queue):
    handler = logging.handlers.QueueHandler(
        queue)  # Just the one handler needed
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(_getLevel(LOGGER_CONFIG['level']))


def log_listener(queue):
    """
    Listen log messages.

    Input:
    - queue: A multiprocessing.Queue which holds temporary log messages.
    """
    abs_path = os.path.abspath(__file__)
    abs_dir = os.path.dirname(abs_path)
    abs_dir = os.path.join(os.path.dirname(abs_dir), 'log')
    name = datetime.datetime.now().strftime("%y-%m-%d")
    log_path = os.path.join(abs_dir, name)
    configure_listener_logger(log_path)
    while True:
        try:
            record = queue.get()
            if record is None:  # End sentinel
                break
            logger = logging.getLogger(record.name)
            # No level or filter logic applied - just do it!
            logger.handle(record)
        except Exception as e:
            if os.name == 'nt':
                print('exception in logger: %s' % e)
            else:
                import syslog
                syslog.syslog('Exception in logger: %s' % e)
