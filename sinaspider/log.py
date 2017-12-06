"""
A logger initializer.
"""

import logging
import logging.handlers
import os

from config import CONFIG

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
        return a colorful output in console
        :param record:
        :return:
        """
        level_name = record.levelname
        msg = logging.Formatter.format(self, record)
        return _COLORERD_LOG_FMAT.get(level_name, '%s') % msg


def init_log(log_path, level=logging.INFO, when="D", backup=7):
    """
    Initialize logging facility.

    Input:
    - log_path: A string of absolute log file path.
    - when: A single charactor of 'S', 'M', 'H', 'D', 'W' to indicate how to split 
            the log file by time interval.

            'S' : Seconds
            'M' : Minutes
            'H' : Hours
            'D' : Days
            'W' : Week day
            default value: 'D'

    Returns True if initialize the logger successfully.
    """
    fmt = CONFIG['LOGGER']['format']
    datefmt = CONFIG['LOGGER']['datefmt']
    formatter = logging.Formatter(fmt, datefmt)
    stream_formatter = ColoredFormatter(fmt, datefmt)
    logger = logging.getLogger()
    logger.setLevel(level)

    dir_path = os.path.dirname(log_path)
    if not os.path.isdir(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            logger.error("failed to create log directory %s: %s" % (dir_path, e))
            return False
    try:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)
    except Exception as e:
        logger.error("failed to add stream handler to logger: %s" % e)
    try:
        handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log", when=when,
                                                            backupCount=backup)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except IOError as e:
        logger.error("faild to add file handler to logger:  %s" % e)
        return False
    return True
