"""
A multithread downloader.
"""

import logging
import requests
import threading
import time
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

from sinaspider.config import *
import sinaspider.scheduler
from sinaspider.services.scheduler_service import Client
from sinaspider.services.ttypes import *
from sinaspider.sina_login import SinaSessionLoginer


class Downloader(threading.Thread):
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
        threading.Thread.__init__(self, name=name, daemon=True)
        self.callbacks = list()             # Called per link downloaded
        self.session = requests.Session()   # Store login session
        self.links = list()                 # Downloading links
        self.user_identity = None           # user name and password
        self.loginer = SinaSessionLoginer(
            self.session)  # Login when session expired
        self.logger = None

    def run(self):
        """
        Entry of the downloader. The main working loop.
        """
        self.logger = logging.getLogger(self.name)
        self.logger.info('Starting %s' % self.name)
        transport = TSocket.TSocket(SCHEDULER_CONFIG['addr'],
                                    SCHEDULER_CONFIG['port'])
        transport = TTransport.TBufferedTransport(
            transport)
        protocol = TBinaryProtocol.TBinaryProtocol(
            transport)
        client = Client(protocol)
        while True:
            try:
                self.logger.debug('Connecting scheduler_service %s' %
                              SCHEDULER_CONFIG['addr'])
                transport.open()
                self.logger.debug('Connected.')
                client.register_downloader(self.name)
                self.logger.debug('Registered')
                self.user_identity = client.request_user_identity()
                self.logger.debug('Get user_identity %s' % str(self.user_identity))
                break
            except TTransport.TTransportException:
                self.logger.exception('Exception while initializing %s, reconecting...' % self.name)
        while True:
            try:
                if not transport.isOpen():
                    transport.open()
                self.links = client.grab_links(
                    DOWNLOADER_CONFIG['link_batch_size'])
                self.logger.debug('Grab links: %s' % self.links)
                for _ in range(len(self.links)):
                    link = self.links[-1]
                    self.logger.debug('Downloading %s.' % link)
                    response = self._download(link)
                    if not response:
                        break
                    for callback in self.callbacks:
                        self.logger.debug(
                            'Running callback: %s.' % callback.__name__)
                        response = callback(response)
                    del self.links[-1]
            except TTransport.TTransportException:
                self.logger.exception('Exception in downloading loop: ')
        if transport.isOpen():
            client.submit_links(self.links)
            client.unregister_downloader(self.name)
            transport.close()
        self.logger.info('Downloader %s stopped.' % self.name)

    def stop(self):
        """
        Stop the downloader.
        """
        self._stop()

    def register_callback(self, func):
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
        self.callbacks.append(func)
        self.logger.info('Register callback %s.' % func.__name__)

    def _download(self, link):
        """
        Download the link.

        Input:
        - link: A string of link to be downloaded.
        """
        while not self._is_stopped:
            try:
                response = self.session.get(link)
                if self._is_login(response):
                    return response
                self.logger.info('Session expired. Relogin...')
                self.loginer.login(self.user_identity)
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

    def __init__(self, callbacks, log_queue):
        """
        Create a downloader pool and initialize the downloaders. 

        Input:
        - callbacks: A list of callbacks to be called when a link is downloaded.
        """
        num_loaders = DOWNLOADER_CONFIG['number_of_downloaders']
        self.downloaders = list()
        for idx in range(num_loaders):
            name = DOWNLOADER_CONFIG['name_prefix'] + '-' + str(idx)
            downloader = Downloader(name)
            for callback in callbacks:
                downloader.register_callback(callback)
            self.downloaders.append(downloader)
        self.log_queue = log_queue

    def start(self):
        """
        Start all of the downloaders.
        """
        sinaspider.log.configure_logger(self.log_queue)
        for downloader in self.downloaders:
            downloader.start()
        while True:
            time.sleep(10)

    def stop(self):
        """
        Stop all of the downloaders.
        """
        for downloader in self.downloaders:
            downloader.stop()
