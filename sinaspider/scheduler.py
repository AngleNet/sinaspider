"""
A simple scheduler.
"""

import logging
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
        self.links = list() # Keep all of links
        self.downloaders = dict() # Keep alive downloaders along with other resources
        self.user_identities = set() # Keep unused user identities
        self.idle_proxies = set() # Keep idle proxies
        self.proxies = set() # Keeps all of proxies
    
    def init(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        for ident in SCHEDULER_CONFIG['user_identity']:
            ident = ttypes.UserIdentity(ident['name'], ident['pwd'])
            self.user_identities.add(ident)

    def register_downloader(self, name):
        """
        Register the downloader along with the name.

        Parameters:
         - name
        """
        self.logger.debug('Register %s' % name)
        return ttypes.RetStatus.SUCCESS

    def unregister_downloader(self, name):
        """
        Unregister the named downloader.

        Parameters:
         - name
        """
        self.logger.debug('Unregister downloader %s' % name)
        return ttypes.RetStatus.SUCCESS

    def request_user_identity(self, name):
        """
        Get a pair of user name and password. For now, each pair of user name and
        password can only be granted to exactly one downloader.
        """
        return ttypes.UserIdentity('test', 'password')

    def resign_user_identity(self, pair, name):
        """
        Give up the user identity.

        Parameters:
         - pair
        """
        return ttypes.RetStatus.SUCCESS

    def grab_links(self, size):
        """
        Grab a batch of links.

        In FIFO order.
        Parameters:
         - size
        """
        return ['http://www.tldp.org/HOWTO/3-Button-Mouse-1.html']

    def submit_links(self, links):
        """
        Submit a batch of links.

        Parameters:
         - links
        """
        return ttypes.RetStatus.SUCCESS

    def request_proxy(self, name):
        """
        Request a living proxy.
        """
        return ttypes.ProxyAddress('221.207.30.251', 80)

    def resign_proxy(self, addr, name):
        """
        Resign a proxy. If a downloader find out the proxy is dead, tell the scheduler.
        The scheduler will give it a new one.

        Parameters:
         - addr
        """
        return ttypes.ProxyAddress('test_addr', 80)

    def submit_proxies(self, addrs):
        """
        Submit a batch of proxies to scheduler.

        Parameters:
         - addrs
        """
        return ttypes.RetStatus.SUCCESS


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

    def submit_links(self, link):
        self.queue.put(link)

    def stop(self):
        """
        Stop the client.
        """
        self.running = False
