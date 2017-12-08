"""
A multithread downloader.
"""

import threading

from sinaspider.config import CONFIG

class Downloader(object):
    """
    A simple downloader. The downloader starts, it grabs a batch of links from 
    the master scheduler and starts to download the web page. When downloaded, 
    call the finish callback. The downloader should do any redirects if needed.
    """
    def __init__(self):
        pass
    
    def run(self):
        """
        Entry of the downloader. The main working loop.
        """
        pass
    
    # TODO: implement the link grab in the scheduler.
    
    def register_finish_callback(self, func):
        """
        Register a finish callback. If multiple callbacks have been registered,
        the callback will be chained. When the final callback gets called, the 
        downloader will throw away the return value.

        Input:
        - func: A function of the prototype:
                def func(Response):
                    doing some dirty work
                    return Response
        Return True if we safely add this callback to the downloader.
        """
        pass
    
    def register_login_callback(self, func):
        """
        Register a login callback. 
        """
        pass

class DownloaderPool(object):
    """
    A downloader pool. The pool has a handful of downloader threads.
    """
    def __init__(self):
        """
        Create a downloader pool and initialize the downloaders. 
        """
        pass
    
    def start(self):
        """
        Start all of the downloaders.
        """
        pass
    
    def stop(self):
        """
        Stop all of the downloaders.
        """
        pass

