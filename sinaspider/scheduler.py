"""
A simple scheduler.
"""

import logging
import multiprocessing
import os
import queue
import thrift

import sinaspider.services.scheduler_service as scheduler_service
import sinaspider.services.ttypes as ttypes
from sinaspider.config import *

logger = logging.getLogger(__name__)

class SchedulerServiceHandler(scheduler_service.Iface):
    """
    A scheduler service.
    """
    def register_downloader(self, name):
      """
      Register the downloader along with the name.

      Parameters:
       - name
      """
      logger.debug('Register downloader %s' % name)
      return ttypes.RetStatus.SUCCESS

    def unregister_downloader(self, name):
      """
      Unregister the named downloader.

      Parameters:
       - name
      """
      logger.debug('Unregister downloader %s' % name)
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
    def __init__(self):
      multiprocessing.Process.__init__(self, name=self.__class__.__name__)
      self.host = SCHEDULER_CONFIG['addr']
      self.port = SCHEDULER_CONFIG['port']
    
    def run(self):
      logger.info('Starting %s' % self.name)
      handler = SchedulerServiceHandler()
      processor = scheduler_service.Processor(handler)
      server_transport = thrift.transport.TSocket.TServerSocket(self.host,
                            self.port)
      tfactory = thrift.transport.TTransport.TBufferedTransportFactory()
      pfactory = thrift.protocol.TBinaryProtocol.TBinaryProtocolFactory()
      tserver = thrift.server.TServer.TSimpleServer(processor, server_transport,
                     tfactory, pfactory)
      logger.info('Serving requests...')
      tserver.serve()
      logger.info('Service %s stopped.' % self.name)
  

class SchedulerServiceClient(multiprocessing.Process):
    """
    A scheduler client daemon.
    """
    def __init__(self):
      multiprocessing.Process.__init__(self, name='SchedulerServiceClient', daemon=True)
      self._links_queue = multiprocessing.Queue(-1)
      self._proxy_queue = multiprocessing.Queue(-1)
    
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
          logger.info('Starting %s' % self.name)
          transport = thrift.transport.TSocket.TSocket(SCHEDULER_CONFIG['addr'], 
                                                       SCHEDULER_CONFIG['port'])
          transport = thrift.transport.TTransport.TBufferedTransport(transport)
          protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(transport)
          client = scheduler_service.Client(protocol)
          logger.debug('Connecting scheduler_service %s' % SCHEDULER_CONFIG['addr'])
          transport.open()
          logger.debug('Conencted.')
          while True:
            links = self._links_queue.get()
            proxies = self._proxy_queue.get()
            client.submit_links(links)
            client.submit_links(proxies)
            logger.debug('Submit links: %s' % links)
            logger.debug('Submit proxies: %s' % str(proxies))
      except thrift.transport.TTransport.TTransportException:
          logging.exception('Exception in connecting to scheduler.') 
      if transport.isOpen():
          client.unregister_downloader(self.name)
          transport.close()
      logger.info('%s stopped.' % self.name)

  

