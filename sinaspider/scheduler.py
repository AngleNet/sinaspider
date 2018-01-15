"""
A simple scheduler.
"""

import logging
import os
from os.path import abspath, dirname, join, isdir
import pickle
import plyvel
import requests
import signal
import time
import threading
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket
from thrift.server import TServer

import sinaspider.log
import sinaspider.services.scheduler_service as scheduler_service
import sinaspider.services.ttypes as ttypes
from sinaspider.config import *
import sinaspider.utils
import sinaspider.sina_pipeline
from sinaspider.downloader import DownloaderType

class LinkType:
    LINK = 0
    TOPIC_LINK = 1

class SchedulerServiceHandler(scheduler_service.Iface):
    """
    A scheduler service.
    """

    def __init__(self):
        self.logger = None
        self.downloaders = dict() # Keep alive downloaders along with other resources
        self.user_identities = set() # Keep unused user identities
        self.idle_proxies = set()
        self.proxies = set() # Keeps all of proxies
        self.proxy_lock = threading.Lock()
        self.cookies = dict()
        self.idle_cookies = set()
        self._link_batch_size = 0
        self.topic_links = list()
        self.links = set()
        self.ready_links_db = None
        self.dead_links_db = None
        self.dead_topic_links_db = None
        self._db_dir = join(dirname(dirname(abspath(__file__))), 'database')
        if not isdir(self._db_dir):
            os.makedirs(self._db_dir)
   
    def init(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        for ident in SCHEDULER_CONFIG['user_identity']:
            ident = ttypes.UserIdentity(ident['name'], ident['pwd'])
            self.user_identities.add(ident)
        self.links_db = plyvel.DB(join(self._db_dir, 'links.db'),
                                        create_if_missing=True)
        for k, v in self.links_db:
            ltype = pickle.loads(v)
            link = pickle.loads(k)
            if ltype == LinkType.LINK:
                self.links.add(link)
            elif ltype == LinkType.TOPIC_LINK:
                self.topic_links.append(link)
            else:
                self.logger.error('Find unkown type link (%s, %s) when recover links' % (link, ltype))
        self.dead_links_db = plyvel.DB(join(self._db_dir, 'dead_links.db'),
                                        create_if_missing=True)
        self.dead_topic_links_db = plyvel.DB(join(self._db_dir, 'dead_topic_links.db'),
                                            create_if_missing=True)

    def close(self):
        for k, v in self.links_db:
            self.links_db.delete(k)
        for link in self.links:
            self.links_db.put(pickle.dumps(link), pickle.dumps(LinkType.LINK))
        for link in self.topic_links:
            self.links_db.put(pickle.dumps(link), pickle.dumps(LinkType.TOPIC_LINK))
        self.links_db.close()
        self.dead_links_db.close()
        self.dead_topic_links_db.close()
        self.logger.info('%s links left, %s topic links left' % (len(self.links), 
                        len(self.topic_links)))

    def register_downloader(self, name):
        """
        Register the downloader along with the name.

        Parameters:
         - name
        """
        self.logger.debug('Register %s' % name)
        if name not in self.downloaders:
            self.downloaders[name] = dict(user_identity=None)
        else:
            self.logger.warn('Downloader %s has been registered.' % name)
        return ttypes.RetStatus.SUCCESS

    def unregister_downloader(self, name):
        """
        Unregister the named downloader. Reclaim all of resources of the downloader.

        Parameters:
         - name
        """
        self.logger.debug('Unregister downloader %s' % name)
        if name not in self.downloaders:
            self.logger.warn('Unregister a never registered downloader: %s' % name)
            return ttypes.RetStatus.FAILED
        user = self.downloaders[name]['user_identity']
        if user:
            self.user_identities.add(user)
            self.logger.debug('Reclaim user identity: %s' % str(user))
        del self.downloaders[name]
        return ttypes.RetStatus.SUCCESS

    def request_user_identity(self, name):
        """
        Get a pair of user name and password. For now, each pair of user name and
        password can only be granted to exactly one downloader.
        """
        ident = self.downloaders[name]['user_identity']
        if ident:
            self.logger.warn('%s already has %s. Remind it.' % (name, ident))
            return ident
        if len(self.user_identities) == 0:
            self.logger.warn('User identity exhausted. Start over.')
            for ident in SCHEDULER_CONFIG['user_identity']:
                ident = ttypes.UserIdentity(ident['name'], ident['pwd'])
                self.user_identities.add(ident)
        ident = self.user_identities.pop() 
        self.downloaders[name]['user_identity'] = ident
        self.logger.debug('Allocate %s for %s' % (ident, name))
        return ident 

    def resign_user_identity(self, pair, name):
        """
        Give up the user identity.

        Parameters:
         - pair
        """
        ident = self.downloaders[name]['user_identity']
        if ident != pair:
            self.logger.warn('%s try to resign %s not owned by itself.' % (name, pair))
            return ttypes.RetStatus.FAILED
        self.user_identities.add(pair)
        self.logger.debug('%s renounces %s' % (name, pair))
        return ttypes.RetStatus.SUCCESS

    def grab_links(self, size):
        """
        Grab a batch of links.

        In FIFO order.
        Parameters:
         - size
        """
        self._link_batch_size = size
        ret_links = []
        for _ in range(min(size, len(self.links))):
            link = self.links.pop()
            ret_links.append(link)
            self.dead_links_db.put(pickle.dumps(link), b'')
        self.logger.info('%s links left' % len(self.links))
        return ret_links 

    def submit_links(self, links):
        """
        Submit a batch of links.

        Parameters:
         - links
        """
        count = 0
        for link in links:
            if link in self.links or self.dead_links_db.get(pickle.dumps(link)) == b'':
                self.logger.debug('bypass: %s' % link)
                continue
            self.links.add(link)
            count += 1
        self.logger.debug('Receive %s links' % count)
        return ttypes.RetStatus.SUCCESS

    def grab_topic_links(self, size):
        """
        Grab a batch of links.

        In FIFO order.
        Parameters:
         - size
        """
        ret_links = []
        for _ in range(min(size, len(self.topic_links))):
            link = self.topic_links.pop(0)
            ret_links.append(link)
        self.logger.debug('%s topic links left.' % len(self.topic_links))
        return ret_links

    def submit_topic_links(self, links):
        """
        Submit a batch of links.

        Parameters:
         - links
        """
        _links = []
        for link in links:
            if 'p/100808' in link and \
                self.dead_topic_links_db.get(pickle.dumps(link)) == b'':
                continue
            _links.append(link)
        self.topic_links.extend(_links)
        self.logger.debug('Receive %s topic links' % len(_links))
        return ttypes.RetStatus.SUCCESS
 
    def request_proxies(self, name, size):
        """
        Request a batch of living proxies.

        Parameters:
         - name
         - size
        """
        self.logger.info('%s requests %s proxies' % (name, size))
        proxies = list()
        if len(self.idle_proxies) < size:
            self.idle_proxies.update(self.proxies)
        for _ in range(min(len(self.idle_proxies), size)):
            proxy = self.idle_proxies.pop()
            proxies.append(proxy)
        left_size = len(self.idle_proxies)
        self.logger.info('%s proxies left' % left_size)
        return  proxies
        
    def request_cookie(self, name):
        """
        Request a cookie.

        Parameters:
         - name
        """
        if len(self.idle_cookies) == 0:
            self.logger.warn('Cookies exhausted. Start over.')
            for v in self.cookies.values():
                self.idle_cookies.add(v)
        if len(self.idle_cookies) == 0:
            return ttypes.Cookie('NULL', '')
        cookie = self.idle_cookies.pop()
        self.logger.debug('Allocate %s for %s' % (cookie, name))
        return cookie

    def submit_cookies(self, cookies):
        """
        Submit cookies.

        Parameters:
         - cookies
        """
        self.logger.info('Receive %s cookies' % len(cookies))
        self.idle_cookies.clear()
        for cookie in cookies:
            self.cookies[cookie.user] = cookie
            self.idle_cookies.add(cookie)
        return ttypes.RetStatus.SUCCESS

    ## Utility methods
    def update_proxies_callback(self):
        self.logger.info('Start updating proxies...')
        ret = requests.get(SCHEDULER_CONFIG['proxy_provider'] % SCHEDULER_CONFIG['proxy_pool_size'])
        new_proxies = set()
        for entry in ret.text.split('\n'):
            addr, port = entry.split(':')
            proxy = ttypes.ProxyAddress(addr, int(port))
            new_proxies.add(proxy)
        self.logger.debug('Number of new proxies: %s' % len(new_proxies))
        self.proxies = new_proxies

class SchedulerServerDaemon(sinaspider.utils.Daemon, TServer.TServer):
    """
    A Scheduler service server.
    """

    def __init__(self, pidfile):
        sinaspider.utils.Daemon.__init__(
            self, pidfile, self.__class__.__name__)
        self.host = SCHEDULER_CONFIG['addr']
        self.port = SCHEDULER_CONFIG['port']
        self.handler = SchedulerServiceHandler()
        processor = scheduler_service.Processor(self.handler)
        server_transport = TSocket.TServerSocket(self.host,
                                                 self.port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        TServer.TServer.__init__(self, processor, server_transport,
                                 tfactory, pfactory)
        self._is_alive = False
        self.timer = sinaspider.utils.RepeatingTimer(SCHEDULER_CONFIG['proxy_interval'], self.handler.update_proxies_callback)

    def run(self):
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        sinaspider.log.configure_logger('.scheduler.log')
        logger = logging.getLogger(self.name)
        self.handler.init()
        self.timer.start()
        self._is_alive = True
        interval = SCHEDULER_CONFIG['server_failover_interval']
        while self._is_alive:
            try:
                logger.info('Starting %s' % self.name)
                logger.info('Serving requests...')
                self.serverTransport.listen()
                while self._is_alive:
                    client = self.serverTransport.accept()
                    if not client:
                        continue
                    client.handle.settimeout(20)
                    itrans = self.inputTransportFactory.getTransport(client)
                    otrans = self.outputTransportFactory.getTransport(client)
                    iprot = self.inputProtocolFactory.getProtocol(itrans)
                    oprot = self.outputProtocolFactory.getProtocol(otrans)
                    try:
                        while self._is_alive:
                            self.processor.process(iprot, oprot)
                    except TTransport.TTransportException:
                        pass
                    except Exception as e:
                        logger.exception(e)
                        self.serverTransport.handle.shutdown(2)
                        self.serverTransport.handle.close()
                        itrans.close()
                        otrans.close()
                        break
                    itrans.close()
                    otrans.close()
            except Exception:
                if self._is_alive:
                    logger.exception(
                        'Failed. Restarting in %s seconds...' %
                        interval)
                    time.sleep(interval)
        self.handler.close()
        logger.info('Service stopped.')

    def sig_handler(self, sig, func):
        self._is_alive = False
        self.serverTransport.close()
        self.timer.stop()

class SchedulerServiceClient(object):
    """
    A scheduler client daemon.
    """

    def __init__(self, queue):
        """
        Input:
        - queue: A multiprocessing.Queue
        """
        self.queue = queue
        self.name = self.__class__.__name__
        self.transport = TSocket.TSocket(SCHEDULER_CONFIG['addr'],
                                         SCHEDULER_CONFIG['port'])
        self.transport = TTransport.TBufferedTransport(self.transport)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = scheduler_service.Client(protocol)
        self.running = False

    def run(self):
        """
        Start entry.
        """
        logger = logging.getLogger(self.name)
        logger.info('Starting %s' % self.name)
        interval = SCHEDULER_CONFIG['client_failover_interval']
        self.running = True
        while self.running:
            try:
                links, dtype = self.queue.get()
                # Need to close the connection later to avoid unparallel
                # threading.
                if not self.transport.isOpen():
                    self.transport.open()
                if dtype == DownloaderType.LINK_DOWNLOADER:
                    self.client.submit_links(links)
                elif dtype == DownloaderType.TOPIC_DOWNLOADER:
                    self.client.submit_topic_links(links)
                self.transport.close()
                logger.debug('Submit links: %s' % links)
            except TTransport.TTransportException:
                logger.exception('Failed. Restarting in %s seconds' % interval)
                time.sleep(interval)
        if self.transport.isOpen():
            self.transport.close()
        logger.info('%s stopped.' % self.name)

    def submit_links(self, links, dtype=DownloaderType.LINK_DOWNLOADER):
        self.queue.put((links, dtype))

    def stop(self):
        """
        Stop the client.
        """
        self.running = False
