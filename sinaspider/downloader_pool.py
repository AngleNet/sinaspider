"""
A multithread downloader.
"""

import logging
import requests
import threading
import thrift

from sinaspider.config import * 
import sinaspider.scheduler
from sinaspider.services.scheduler_service import Client

logger = logging.getLogger(__name__)

class Downloader(object):
    """
    A simple downloader. The downloader starts, it grabs a batch of links from 
    the master scheduler and starts to download the web page. When downloaded, 
    call the finish callback. The downloader should do any redirects if needed.
    """
    def __init__(self, name):
        """
        Input:
        - name: A string of downloader name.
        """
        self.name = name
        self.callbacks = dict(finish=list(),
                              login=None)

        self.session = requests.Session()
        self.links = list()

        self._stop_event = threading.Event()
    
    def run(self):
        """
        Entry of the downloader. The main working loop.
        """
        try:
            transport = thrift.transport.TSocket.TSocket(SCHEDULER_CONFIG['addr'], 
                                                         SCHEDULER_CONFIG['port'])
            transport = thrift.transport.TTransport.TBufferedTransport(transport)
            protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(transport)
            client = Client(protocol)
            transport.open()
            client.register_downloader(self.name)
            
            while True:
                self.links = client.grab_links(DOWNLOADER_CONFIG['link_batch_size'])
                for _ in range(len(self.links)):
                    link = self.links[-1]
                    response = self._download(link)
                    if not response:
                        break
                    for callback in self.callbacks['finish']:
                        callback(response)
                    del self.links[-1]
        except thrift.transport.TTransport.TTransportException:
            if transport.isOpen():
                transport.close()
            logging.exception('Exception in connecting to scheduler.') 
        logger.info('Downloader %s stopped.' % self.name)
        if transport.isOpen():
            client.submit_links(self.links)
            client.unregister_downloader(self.name)
            transport.close()
    
    def stop(self):
        """
        Stop the downloader.
        """
        self._stop_event.set()
        
    
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
        """
        self.callbacks['finish'].append(func)
    
    def register_login_callback(self, func):
        """
        Register a login callback. 

        Input:
        - func: A function of the prototype:
                def func(Response):
                    doing some dirty work
                    return Response
        """
        self.callbacks['login'] = func
    
    def _download(self, link):
        """
        Download the link.

        """
        while not self._stop_event.isSet():
            try:
                response = self.session.get(link)
                if self._is_login(response):
                    return response
                login_callback = self.callbacks['login']
                login_callback(self.session)
            except requests.RequestException:
                logging.exception('Exception in downloading %s' % link)
        return None
    
    def _is_login(self, response):
        """
        Should be implemented inline.
        """
        return True

    

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

