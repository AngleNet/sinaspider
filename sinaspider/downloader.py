"""
A multithread downloader.
"""

import logging
import random
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

        self.proxy_lock = threading.Lock() # Protects updating of proxies.
        self.proxies = None
        self.transport = TSocket.TSocket(SCHEDULER_CONFIG['addr'],
                                    SCHEDULER_CONFIG['port'])
        self.transport = TTransport.TBufferedTransport(
            self.transport)
        protocol = TBinaryProtocol.TBinaryProtocol(
            self.transport)
        self.client = Client(protocol)
 

    def run(self):
        logger = logging.getLogger(self.name)
        logger.info('Starting %s' % self.name)
        self.downloading = True
        interval = SCHEDULER_CONFIG['client_failover_interval']
        while self.downloading:
            try:
                if not self.transport.isOpen():
                    self.transport.open()
                self.client.register_downloader(self.name)
                logger.debug('Registered')
                self.user_identity = self.client.request_user_identity(self.name)
                logger.debug('Get user identity: %s' % self.user_identity)
                self.transport.close()
                break
            except TTransport.TTransportException:
                logger.exception(
                    'Exception while initializing, reconecting...')
                time.sleep(interval)
            except Exception:
                logger.exception('Failed while initialization, exitting...')
                self.downloading = False 

        while self.downloading:
            try:
                if not self.transport.isOpen():
                    self.transport.open()
                self.links = self.client.grab_links(
                    DOWNLOADER_CONFIG['link_batch_size'])
                self.transport.close()
                if len(self.links) == 0:
                    logger.warn('No links available. Waiting...')
                    time.sleep(interval)
                    continue
                logger.debug('Grab links: %s' % self.links)
                for _ in range(len(self.links)):
                    link = self.links[-1]
                    logger.debug('Downloading %s.' % link)
                    response = self._download(link)
                    if not response:
                        break  # Exiting
                    self.pipeline.feed(response)
                    del self.links[-1]
                    time.sleep(5)
            except TTransport.TTransportException:
                logger.exception('Connection error.')
            except Exception:
                logger.exception('Unkown failure, exiting...')
                self.downloading = False


        if self.transport.isOpen():
            # TODO: submit uncrawled links to scheduler.
            logger.debug('Unregiser downloader.')
            self.client.unregister_downloader(self.name)
            logger.debug('Closing connection to scheduler.')
            self.transport.close()
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
                if 'https://weibo.com/sorry?sysbusy' in response.url:
                    logger.debug('Too fast. Waiting...')
                    time.sleep(DOWNLOADER_CONFIG['sysbusy_wait_latency'])
                    continue
                if self._is_login(response):
                    return response
                logger.info('Session expired. Relogin...')
                #self.loginer.login(self.user_identity)
                self._update_cookie()
            except requests.RequestException:
                logger.exception('Exception in downloading %s' % link)
        return None  # Exiting

    def _is_login(self, response):
        """
        Should be implemented inline.
        """
        url = response.url.split('?')[0]
        if 'passport.weibo.com/visitor/visitor' in url:
            return False
        return True

    def _update_cookie(self):
        logger = logging.getLogger(self.name)
        cookie = None
        while True:
            time.sleep(DOWNLOADER_CONFIG['cookie_update_interval'])
            if not self.transport.isOpen():
                self.transport.open()
            cookie = self.client.request_cookie(self.name)
            self.transport.close()
            if cookie.user != 'NULL':
                break
            logger.info('No cookies. Retry later...')
        logger.info('Get cookie: %s' % cookie) 
        cookie_dict = dict()
        for entry in cookie.cookie.split(';'):
            if entry == '':
                continue
            _idx = entry.find('=')
            key = entry[:_idx]
            value = entry[_idx+1:]
            cookie_dict[key] = value
        self.session.cookies = requests.cookies.cookiejar_from_dict(cookie_dict)

    def stop(self):
        self.downloading = False

    def update_proxy_list_callback(self):
        """
        Update the proxy list later via the timer.
        """
        self.proxy_lock.acquire()
        if not self.transport.isOpen():
            self.transport.open()

