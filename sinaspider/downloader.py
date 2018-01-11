"""
A multithread downloader.
"""

import aenum
import logging
import random
import requests
import threading
import time
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket
import uuid

from sinaspider.config import *
from sinaspider.services.scheduler_service import Client
from sinaspider.services.ttypes import *
from sinaspider.sina_login import SinaSessionLoginer


class DownloaderType(aenum.Enum):
    TOPIC_DOWNLOADER = 0 # Trending topics
    LINK_DOWNLOADER = aenum.auto() # Normal

    UNDEFINED = aenum.auto()


class Downloader(threading.Thread):
    """
    A simple downloader. The downloader starts, it grabs a batch of links from
    the master scheduler and starts to download the web page. When downloaded,
    call the finish callback. The downloader should do any redirects if needed.
    """

    def __init__(self, name, pipeline, dtype):
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
        self.proxies = set() 
        self.transport = TSocket.TSocket(SCHEDULER_CONFIG['addr'],
                                    SCHEDULER_CONFIG['port'])
        self.transport = TTransport.TBufferedTransport(
            self.transport)
        protocol = TBinaryProtocol.TBinaryProtocol(
            self.transport)
        self.client = Client(protocol)
        if dtype == DownloaderType.LINK_DOWNLOADER:
            self.link_graber = self.client.grab_links
        elif dtype == DownloaderType.TOPIC_DOWNLOADER:
            self.link_graber = self.client.grab_topic_links
        elif dtype == DownloaderType.UNDEFINED:
            import os
            os._exit(1)
        self.downloader_type = dtype
 

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
                self.links = self.link_graber(
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
                time.sleep(interval)
            except Exception:
                logger.exception('Unkown failure, exiting...')
                self.downloading = False

        if self.transport.isOpen():
            # TODO: submit uncrawled links to scheduler.
            logger.debug('Unregiser downloader.')
            self.client.unregister_downloader(self.name)
            if len(self.links) > 0:
                if self.downloader_type == DownloaderType.LINK_DOWNLOADER:
                    for idx in range(len(self.links)):
                        patch = '&uuid=%s' % uuid.uuid4().hex
                        self.links[idx] = self.links[idx] + patch
                    self.client.submit_links(self.links)
                elif self.downloader_type == DownloaderType.TOPIC_DOWNLOADER:
                    self.client.submit_topic_links(self.links)
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
        _proxy = None
        while self.downloading:
            try:
                if len(self.proxies) == 0:
                    time.sleep(0.5)
                    logger.debug('No proxies, waiting...')
                    continue
                self.proxy_lock.acquire()
                proxy = random.choice(self.proxies)
                self.proxy_lock.release()
                if _proxy != proxy:
                    self.session.close()
                logger.debug('Using proxy: %s' % proxy)
                response = self.session.get(link, proxies=proxy, 
                            timeout=DOWNLOADER_CONFIG['requests_timeout'],
                            verify=False)
                if 'weibo.com/sorry?sysbusy' in response.url:
                    #logger.debug('Too fast. Waiting...')
                    #time.sleep(DOWNLOADER_CONFIG['sysbusy_wait_latency'])
                    continue
                if self._is_login(response):
                    return response
                logger.info('Session expired. Relogin...')
                #self.loginer.login(self.user_identity)
                self._update_cookie()
            except (requests.exceptions.ConnectTimeout, 
                    requests.exceptions.ProxyError,
                    # Increase requests_timeout to handle this
                    requests.exceptions.ReadTimeout, 
                    # BadStatusLine aborts the connection
                    requests.exceptions.ConnectionError) as e:
                logger.warn('requests exception: %s' % e)
            except Exception:
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

    def update_proxies_callback(self):
        """
        Update the proxy list later via the timer.
        """
        logger = logging.getLogger(self.name)
        if not self.transport.isOpen():
            self.transport.open()
        proxies = self.client.request_proxies(self.name, 
                        DOWNLOADER_CONFIG['proxy_pool_size'])
        logger.debug('Get proxies: %s' % proxies)
        self.transport.close()
        new_proxies = list()
        for proxy in proxies:
            _proxy = {
                'http': 'http://%s:%s' % (proxy.addr, proxy.port), 
                'https': 'https://%s:%s' % (proxy.addr, proxy.port)
            }
            new_proxies.append(_proxy)
        logger.debug('Old proxies: %s' % self.proxies)
        self.proxy_lock.acquire()
        self.proxies = new_proxies
        self.proxy_lock.release()
        logger.debug('New proxies: %s' % self.proxies)

