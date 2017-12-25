#!/usr/bin/env python

import json
import logging
import multiprocessing
import os
from os.path import abspath, join, dirname
import signal
import sys
import thrift
import time
import uuid
import requests

import sinaspider.config
import sinaspider.downloader
import sinaspider.log
import sinaspider.pipeline
import sinaspider.scheduler
import sinaspider.utils
import sinaspider.services
import sinaspider.sina_pipeline

class CookieUploader(object):
    def start(self):
        addr = sinaspider.config.SCHEDULER_CONFIG['addr']
        port = sinaspider.config.SCHEDULER_CONFIG['port']
        transport = thrift.transport.TSocket.TSocket(addr, port)
        transport = thrift.transport.TTransport.TBufferedTransport(transport)
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(transport)
        client = sinaspider.services.scheduler_service.Client(protocol)
        # Read cookie file
        fcookie = 'cookies.json'
        cookie_dict = None
        with open(fcookie, 'r') as fd:
            cookie_dict = json.load(fd)
        cookies = list()
        for user, cookie in cookie_dict.items():
            cookie = sinaspider.services.ttypes.Cookie(user, cookie)
            cookies.append(cookie)
        transport.open()
        client.submit_cookies(cookies)
        transport.close()

class StoreCookie(object):
    def start(self):
        users = list()
        for ident in sinaspider.config.SCHEDULER_CONFIG['user_identity']:
            ident = sinaspider.services.ttypes.UserIdentity(ident['name'], ident['pwd'])
            users.append(ident)

        cookies = dict()
        print('Start login...')
        for user in users:
            session = requests.Session()
            print('\tLogin %s...' % (user.name), end='')
            loginer = sinaspider.sina_login.SinaSessionLoginer(session)
            loginer.login(user)
            print('SUCCESS')
            cookie = ''
            for key, value in requests.utils.dict_from_cookiejar(session.cookies).items():
                cookie += '%s=%s;' % (key, value)
            cookies[user.name] = cookie
        with open('cookies.json', 'w+') as fd:
            json.dump(cookies, fd)
        print('Complete.')


class SeedLinkSubmitDaemon(sinaspider.utils.Daemon):
    def __init__(self, pid_file):
        sinaspider.utils.Daemon.__init__(
            self, pid_file, self.__class__.__name__)
        self.links = []
        self.links.append(sinaspider.sina_pipeline._TRENDING_TWEETS_LINK + '&uuid=%s')
        self.running = True 
    def run(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        addr = sinaspider.config.SCHEDULER_CONFIG['addr']
        port = sinaspider.config.SCHEDULER_CONFIG['port']
        transport = thrift.transport.TSocket.TSocket(addr, port)
        transport = thrift.transport.TTransport.TBufferedTransport(transport)
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(transport)
        client = sinaspider.services.scheduler_service.Client(protocol)
        self.running = True
        while self.running:
            if not transport.isOpen():
                transport.open()
            patch = uuid.uuid4().hex
            links = [link % patch for link in self.links]
            client.submit_links(links)
            transport.close()
            time.sleep(2)
        transport.close()
 
    def exit_gracefully(self, sig, func):
        self.running = False
 


class SinaSpiderDaemon(sinaspider.utils.Daemon):
    def __init__(self, pid_file):
        sinaspider.utils.Daemon.__init__(
            self, pid_file, self.__class__.__name__)
        self.manager = None
        self.downloaders = list()
        self.engine_server = None

    def run(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        # TODO: Replace os.fork in Daemon to enable the creation of manager
        self.manager = multiprocessing.Manager()
        sinaspider.log.configure_logger('.downloader.log')
        logger = logging.getLogger(self.name)
        logger.info('Init pipeline engine')
        queue = self.manager.Queue(-1)
        pipeline = sinaspider.sina_pipeline.SinaPipeline(queue)
        engine = sinaspider.pipeline.PipelineEngine(pipeline, self.manager)
        self.engine_server = multiprocessing.Process(name=engine.name,
                                                     target=engine.run)
        self.engine_server.start()

        for idx in range(
                sinaspider.config.DOWNLOADER_CONFIG['num_downloaders']):
            name = sinaspider.config.DOWNLOADER_CONFIG['name_prefix'] + '-' + str(
                idx)
            logger.debug('Creating %s' % name)
            downloader = sinaspider.downloader.Downloader(name, pipeline)
            self.downloaders.append(downloader)

        self.timer = sinaspider.utils.RepeatingTimer(
            sinaspider.config.DOWNLOADER_CONFIG['proxy_interval'],
            self.timer_callback
        )
        self.timer.start()

        for downloader in self.downloaders:
            logger.debug('Starting %s thread' % downloader.name)
            downloader.start()

        for downloader in self.downloaders:
            downloader.join()
            logger.debug('%s finished.' % downloader.name)
        self.timer.join()
        logger.info('Daemon stopped')

    def exit_gracefully(self, sig, func):
        logger = logging.getLogger(self.name)
        os.kill(self.engine_server.pid, signal.SIGTERM)
        for downloader in self.downloaders:
            downloader.stop()
        self.timer.stop()
    
    def timer_callback(self):
        logger = logging.getLogger(self.name)
        logger.info('Starting updating proxies of downloaders')
        for downloader in self.downloaders:
            logger.debug('Updating proxies of %s' % downloader.name)
            downloader.update_proxies_callback()
        logger.info('Proxies updated.')
        


if __name__ == '__main__':
    pid_dir = dirname(abspath(__file__))
    if len(sys.argv) == 3:
        daemon = None
        cmd = sys.argv[1]
        target = sys.argv[2]
        pid_file = join(pid_dir, target + '.pid')
        if target == 'scheduler':
            daemon = sinaspider.scheduler.SchedulerServerDaemon(pid_file)
        elif target == 'spider':
            daemon = SinaSpiderDaemon(pid_file)
        elif target == 'seeder':
            daemon = SeedLinkSubmitDaemon(pid_file)
        elif target == 'loginer':
            daemon = StoreCookie()
        elif target == 'uploader':
            daemon = CookieUploader()
        else:
            print("Unknown target")
            sys.exit(2)
        if cmd == 'start':
            daemon.start()
        elif cmd == 'stop':
            daemon.stop()
        elif cmd == 'restart':
            daemon.restart()
        else:
            print('Unknown command')
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart scheduler" % sys.argv[0])
        sys.exit(2)
