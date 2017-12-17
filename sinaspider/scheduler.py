"""
A simple scheduler.
"""

import logging
from os.path import abspath, dirname, join
import pickle
import plyvel
import signal
import time
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket
from thrift.server import TServer

import sinaspider.log
import sinaspider.services.scheduler_service as scheduler_service
import sinaspider.services.ttypes as ttypes
from sinaspider.config import *
import sinaspider.utils


class SchedulerServiceHandler(scheduler_service.Iface):
    """
    A scheduler service.
    """

    def __init__(self):
        self.logger = None
        self.num_links = 0
        self.downloaders = dict() # Keep alive downloaders along with other resources
        self.user_identities = set() # Keep unused user identities
        self.idle_proxies = set() # Keep idle proxies
        self.proxies = set() # Keeps all of proxies
        self.ready_links_generator = None
        self._link_batch_size = 0
        self.ready_links_db = None
        self.dead_links_db = None
    
    def init(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        for ident in SCHEDULER_CONFIG['user_identity']:
            ident = ttypes.UserIdentity(ident['name'], ident['pwd'])
            self.user_identities.add(ident)
        db_dir = join(dirname(dirname(abspath(__file__))), 'database')
        self.ready_links_db = plyvel.DB(join(db_dir, 'ready_links.db'), create_if_missing=True)
        self.dead_links_db = plyvel.DB(join(db_dir, 'dead_links.db'), create_if_missing=True)
        self.ready_links_generator = self._ready_links_generator()

    def register_downloader(self, name):
        """
        Register the downloader along with the name.

        Parameters:
         - name
        """
        self.logger.debug('Register %s' % name)
        if name not in self.downloaders:
            self.downloaders[name] = dict(proxies=list(), user_identity=None)
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
        for proxy in self.downloaders[name]['proxies']:
            self.logger.debug('Reclaim proxy: %s' % str(proxy))
            self.idle_proxies.add(proxy)
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
        if self.downloaders[name]['user_identity']:
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
        links = self.ready_links_generator()
        for link in links:
            klink = pickle.dumps(link)
            self.dead_links_db.put(klink, b'')
            self.ready_links_db.delete(klink)
        self.num_links -= len(links)
        self.logger.info('%s links left' % self.num_links)
        return links

    def submit_links(self, links):
        """
        Submit a batch of links.

        Parameters:
         - links
        """
        count = 0
        for link in links:
            klink = pickle.dumps(link)
            if self.dead_links_db.get(klink):
                continue
            self.ready_links_db.put(klink, b'')
            count += 1
        self.num_links += count
        self.logger.debug('Receive %s links' % len(links))
        return ttypes.RetStatus.SUCCESS

    def request_proxy(self, name):
        """
        Request a living proxy.

        """
        proxies = self.downloaders[name]['proxies']
        if len(self.idle_proxies) == 0:
            self.logger.warn('Proxies are exhausted. Start over')
            self.idle_proxies = self.proxies.copy()
        proxy = self.idle_proxies.pop()
        if proxy not in proxies:
            proxies.append(proxy)
        self.logger.debug('Allocate %s for %s' % (proxy, name))
        self.logger.info('%s proxies left.' % len(self.idle_proxies))
        return proxy

    def resign_proxy(self, addr, name):
        """
        Resign a proxy. If a downloader find out the proxy is dead, tell the scheduler.

        Parameters:
         - addr
        """
        proxies = self.downloaders[name]['proxies']
        if addr not in proxies:
            self.logger.warn('%s try to resign %s not owned by itself' % (name, addr))
            return ttypes.RetStatus.FAILED
        proxies.remove(addr)
        self.idle_proxies.add(addr)
        return ttypes.RetStatus.SUCCESS

    def submit_proxies(self, addrs):
        """
        Submit a batch of proxies to scheduler.

        Parameters:
         - addrs
        """
        self.logger.debug('Receive %s proxies' % len(addrs))
        for addr in addrs:
            self.idle_proxies.add(addr)
            self.proxies.add(addr)
        self.logger.debug('%s proxies in total' % len(self.proxies))
        return ttypes.RetStatus.SUCCESS

    ## Utility methods
    def _ready_links_generator(self):
        """
        Return a link
        """
        while True:
            snapshot = self.ready_links_db.snapshot()
            with snapshot.iterator() as it:
                count = 0
                links = []
                for k, v in it:
                    count += 1
                    links.append(pickle.loads(k))
                    if count >= self._link_batch_size:
                        yield links
                        links = []
                        count = 0
            snapshot.close()



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

    def run(self):
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        sinaspider.log.configure_logger('.scheduler.log')
        logger = logging.getLogger(self.name)
        self.handler.init()
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
                    itrans.close()
                    otrans.close()
            except Exception:
                if self._is_alive:
                    logger.exception(
                        'Failed. Restarting in %s seconds...' %
                        interval)
                    time.sleep(interval)
        logger.info('Service stopped.')

    def sig_handler(self, sig, func):
        self._is_alive = False
        self.serverTransport.close()


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
                links = self.queue.get()
                # Need to close the connection later to avoid unparallel
                # threading.
                if not self.transport.isOpen():
                    self.transport.open()
                self.client.submit_links(links)
                self.transport.close()
                logger.debug('Submit links: %s' % links)
            except TTransport.TTransportException:
                logger.exception('Failed. Restarting in %s seconds' % interval)
                time.sleep(interval)
        if self.transport.isOpen():
            self.transport.close()
        logger.info('%s stopped.' % self.name)

    def submit_links(self, links):
        for link in links:
            self.queue.put(link)

    def stop(self):
        """
        Stop the client.
        """
        self.running = False
