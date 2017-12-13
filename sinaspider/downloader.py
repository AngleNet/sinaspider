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

    def __init__(self, name, pipeline):
        """
        Input:
        - name: A string of downloader name.
        """
        threading.Thread.__init__(self, name=name)
        self.session = requests.Session()   # Store login session
        self.links = list()                 # Downloading links
        self.user_identity = None           # user name and password
        self.loginer = SinaSessionLoginer(
            self.session)  # Login when session expired
        self.downloading = False
        self.pipeline = pipeline

    def run(self):
        logger = logging.getLogger(self.name)
        logger.info('Starting %s' % self.name)
        self.downloading = True
        transport = TSocket.TSocket(SCHEDULER_CONFIG['addr'],
                                    SCHEDULER_CONFIG['port'])
        transport = TTransport.TBufferedTransport(
            transport)
        protocol = TBinaryProtocol.TBinaryProtocol(
            transport)
        client = Client(protocol)
        interval = SCHEDULER_CONFIG['client_failover_interval']
        while self.downloading:
            try:
                if not transport.isOpen():
                    transport.open()
                client.register_downloader(self.name)
                logger.debug('Registered')
                self.user_identity = client.request_user_identity()
                logger.debug('Get user identity: %s:%s' %
                             (self.user_identity.name, self.user_identity.pwd))
                break
            except TTransport.TTransportException:
                logger.exception(
                    'Exception while initializing, reconecting...')
                time.sleep(interval)

        while self.downloading:
            try:
                if not transport.isOpen():
                    transport.open()
                self.links = client.grab_links(
                    DOWNLOADER_CONFIG['link_batch_size'])
                if transport.isOpen():
                    transport.close()
                logger.debug('Grab links: %s' % self.links)
                for _ in range(len(self.links)):
                    link = self.links[-1]
                    logger.debug('Downloading %s.' % link)
                    response = self._download(link)
                    if not response:
                        break  # Exiting
                    self.pipeline.feed(response)
                    del self.links[-1]
            except TTransport.TTransportException:
                logger.exception('Exception in downloading loop: ')

        if transport.isOpen():
            # TODO: submit uncrawled links to scheduler.
            logger.debug('Unregiser downloader.')
            client.unregister_downloader(self.name)
            logger.debug('Closing connection to scheduler.')
            transport.close()
        logger.info('Downloader stopped.')

    def _download(self, link):
        """
        Download the link.

        Input:
        - link: A string of link to be downloaded.
        """
        logger = logging.getLogger(self.name)
        while self.downloading:
            try:
                response = self.session.get(link)
                if self._is_login(response):
                    return response
                logger.info('Session expired. Relogin...')
                self.loginer.login(self.user_identity)
            except requests.RequestException:
                logger.exception('Exception in downloading %s' % link)
        return None  # Exiting

    def _is_login(self, response):
        """
        Should be implemented inline.
        """
        return True

    def stop(self):
        self.downloading = False
