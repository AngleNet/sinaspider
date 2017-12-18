#!/usr/bin/env python

import logging
import multiprocessing
import os
from os.path import abspath, join, dirname
import signal
import sys
import thrift
import time
import uuid

import sinaspider.config
import sinaspider.downloader
import sinaspider.log
import sinaspider.pipeline
import sinaspider.scheduler
import sinaspider.utils
import sinaspider.services
import sinaspider.sina_pipeline

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

        for downloader in self.downloaders:
            logger.debug('Starting %s thread' % downloader.name)
            downloader.start()

        for downloader in self.downloaders:
            downloader.join()
            logger.debug('%s finished.' % downloader.name)
        logger.info('Daemon stopped')

    def exit_gracefully(self, sig, func):
        logger = logging.getLogger(self.name)
        os.kill(self.engine_server.pid, signal.SIGTERM)
        for downloader in self.downloaders:
            downloader.stop()


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
