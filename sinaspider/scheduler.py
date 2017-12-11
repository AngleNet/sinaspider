"""
A simple scheduler.
"""

import logging
import multiprocessing
import os
import queue
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket
from thrift.server import TServer

import sinaspider.log
import sinaspider.services.scheduler_service as scheduler_service
import sinaspider.services.ttypes as ttypes
from sinaspider.config import *


class SchedulerServiceHandler(scheduler_service.Iface):
    """
    A scheduler service.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_downloader(self, name):
        """
        Register the downloader along with the name.

        Parameters:
         - name
        """
        self.logger.debug('Register downloader %s' % name)
        return ttypes.RetStatus.SUCCESS

    def unregister_downloader(self, name):
        """
        Unregister the named downloader.

        Parameters:
         - name
        """
        self.logger.debug('Unregister downloader %s' % name)
        return ttypes.RetStatus.SUCCESS

    def request_user_identity(self, ):
        """
        Get a pair of user name and password. For now, each pair of user name and
        password can only be granted to exactly one downloader.
        """
        return ttypes.UserIdentity('test', 'password')

    def resign_user_identity(self, pair):
        """
        Give up the user identity.

        Parameters:
         - pair
        """
        return ttypes.RetStatus.SUCCESS

    def grab_links(self, size):
        """
        Grab a batch of links.

        Parameters:
         - size
        """
        return list('test_link')

    def submit_links(self, links):
        """
        Submit a batch of links.

        Parameters:
         - links
        """
        return ttypes.RetStatus.SUCCESS

    def request_proxy(self, ):
        """
        Request a living proxy.
        """
        return ttypes.ProxyAddress('test_addr', 80)

    def resign_proxy(self, addr):
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


class SchedulerServer(multiprocessing.Process):
    """
    A Scheduler service server.
    """

    def __init__(self, log_queue):
        multiprocessing.Process.__init__(self, name=self.__class__.__name__)
        self.host = SCHEDULER_CONFIG['addr']
        self.port = SCHEDULER_CONFIG['port']
        self.logger = None
        self.log_queue = log_queue

    def run(self):
        try:
            sinaspider.log.configure_logger(self.log_queue)
            self.logger = logging.getLogger(self.name)
            self.logger.info('Starting %s' % self.name)
            handler = SchedulerServiceHandler()
            processor = scheduler_service.Processor(handler)
            server_transport = TSocket.TServerSocket(self.host,
                                                                      self.port)
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            tserver = TServer.TSimpleServer(processor, server_transport,
                                                          tfactory, pfactory)
            self.logger.info('Serving requests...')
            tserver.serve()
            self.logger.info('Service %s stopped.' % self.name)
        except Exception:
            logging.exception('%s failed accidently.' % self.name)


class SchedulerServiceClient(multiprocessing.Process):
    """
    A scheduler client daemon.
    """

    def __init__(self, log_queue):
        multiprocessing.Process.__init__(
            self, name='SchedulerServiceClient', daemon=True)
        self._links_queue = multiprocessing.Queue(-1)
        self._proxy_queue = multiprocessing.Queue(-1)

        self.log_queue = log_queue
        self.logger = None        # logger can only be used in run.

    def submit_links(self, links):
        """
        Input:
        - links: A list of string of link.
        """
        self._links_queue.put(links)

    def submit_proxy(self, proxies):
        """
        Input:
        - proxies: A list of ttypes.ProxyAddress.
        """
        self._proxy_queue.put(self, proxies)

    def run(self):
        """
        Start entry.
        """
        try:
            sinaspider.log.configure_logger(self.log_queue)
            self.logger = logging.getLogger(self.name)
            self.logger.info('Starting %s' % self.name)
            transport = TSocket.TSocket(SCHEDULER_CONFIG['addr'],
                                                         SCHEDULER_CONFIG['port'])
            transport = TTransport.TBufferedTransport(
                transport)
            protocol = TBinaryProtocol.TBinaryProtocol(
                transport)
            client = scheduler_service.Client(protocol)
            self.logger.debug('Connecting scheduler_service %s' %
                              SCHEDULER_CONFIG['addr'])
            transport.open()
            self.logger.debug('Conencted.')
            while True:
                links = self._links_queue.get()
                proxies = self._proxy_queue.get()
                client.submit_links(links)
                client.submit_links(proxies)
                self.logger.debug('Submit links: %s' % links)
                self.logger.debug('Submit proxies: %s' % str(proxies))
        except TTransport.TTransportException:
            logging.exception('Exception in connecting to scheduler.')
        if transport.isOpen():
            client.unregister_downloader(self.name)
            transport.close()
        self.logger.info('%s stopped.' % self.name)
