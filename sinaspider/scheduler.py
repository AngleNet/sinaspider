"""
A simple scheduler.
"""

import logging

import sinaspider.services.scheduler_service as scheduler_service
import sinaspider.services.ttypes as ttypes

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
      logger.info('register downloader: %s' % name)
      return ttypes.RetStatus.SUCCESS

    def unregister_downloader(self, name):
      """
      Unregister the named downloader.

      Parameters:
       - name
      """
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



class Scheduler(object):
    pass