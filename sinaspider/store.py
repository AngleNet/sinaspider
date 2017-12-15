"""
A simple driver for data store.
"""

import plyvel
import threading
from os.path import abspath, join, dirname

class Store(object):
    """
    A store which use leveldb as the backend.
    """
    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self):
        db_dir = dirname(dirname(abspath(__file__)))
        db_path = join(join(db_dir, 'data'), 'weibo.leveldb')
        self.db = plyvel.DB(db_path, create_if_missing=True)

    @staticmethod
    def leveldb(cls):
        """
        Get the leveldb driver.
        """
        if not cls._instance:
            cls._instance_lock.acquire()
            if not cls._instance:
                cls._instance = Store()
            cls._instance_lock.release()
        return cls._instance.db

