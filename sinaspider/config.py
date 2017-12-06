"""
SinaSpider's configration manager.
"""
import json
import os.path

def dir_path():
    """
    Return a string of current directory path.
    """
    abs_path = os.path.abspath(__file__)
    _dir = os.path.dirname(abs_path)
    return _dir
CONFIG = None
with open(os.path.join(dir_path(), 'config.json')) as _config_fd:
    CONFIG = json.load(_config_fd)
