"""
A simple html processing pipeline.
"""

import logging
import concurrent.futures
import os
import signal
import threading

from sinaspider.config import *
import sinaspider.log


class PipelineNode(object):
    """
    A pipeline node.

    Subclass should call PipelineNode.__init__() at first.
    """

    def __init__(self, name):
        self.name = name
        self.children = list()

    def forward(self, node):
        """
        Add a child node.

        Input:
        - node: A PipelineNode to be added.
        """
        self.children.append(node)

    def run(self, *kws, **kwargs):
        """
        Implement this in subclass.
        """
        pass

    def start(self, client, *kws):
        """
        Runs current node.
        """
        kws = self.run(client, *kws)
        if kws: # Empty response. Exit the pipeline 
            for child in self.children:
                child.start(client, *kws)


class Pipeline(object):
    """
    A pipeline is composed of a series of pipeline node. The downloader feed each
    response to the pipeline, the response flow through the pipeline.
    """

    def __init__(self, name, head, queue):
        """
        Input:
        - name: A string of pipeline node name.
        - head: A PipelineNode which acts as an entry of the pipeline.
        - queue: A multiprocess.Queue.
        """
        self.name = name
        self.head = head
        self.scheduler_client = None
        self.queue = queue

    def start(self, response):
        """
        Start the pipeline with the response
        """
        logger = logging.getLogger(self.name)
        try:
            assert self.scheduler_client, 'Scheduler client is not registered yet.'
            self.head.start(self.scheduler_client, response)
        except Exception:
            logger.exception('Exception in pipeline.')

    def eat(self):
        return self.queue.get()

    def feed(self, response):
        self.queue.put(response)

    def register_scheduler_client(self, client):
        self.scheduler_client = client


class PipelineEngine(object):
    """
    A pipeline engine. The engine executes the pipeline by a process pool. Users
    should only feed inputs to the pipeline.
    """

    def __init__(self, pipeline, manager):
        """
        Input:
        - pipeline: A Pipeline which defines a flow of computations.
        - manager: A multiprocessing.Manager.
        """
        self.pipeline = pipeline
        self.manager = manager
        self.name = self.__class__.__name__
        queue = self.manager.Queue(-1)

        self.executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=PIPELINE_CONFIG['engine_pool_size'])
        self.scheduler_client = sinaspider.scheduler.SchedulerServiceClient(
            queue)
        self.scheduler_client_thread = threading.Thread(
            name='SchedulerServiceClient', target=self.scheduler_client.run)
        self.pipeline.register_scheduler_client(self.scheduler_client)
        self.running = False

    def run(self):
        """
        Start entry.
        """
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        sinaspider.log.configure_logger('.engine.log')
        logger = logging.getLogger(self.name)
        logger.info('Starting scheduler client...')
        self.scheduler_client_thread.start()
        logger.info('Running pipeline...')
        self.running = True
        while self.running:
            try:
                response = self.pipeline.eat()
                logger.debug('Processing %s' % str(response))
                self.executor.submit(self.pipeline.start, response)
            except Exception:
                logger.exception('Exception during submit %s' % str(response))
        logger.info('Stopping scheduler client')
        self.scheduler_client_thread.join()
        logger.info('Stopping engine executor.')
        self.executor.shutdown()
        logger.info('Stopped.')

    def sig_handler(self, sig, func):
        """
        Handler for signal.SIGTERM. The handler shuts down the engine executor.
        """
        self.running = False
        self.scheduler_client.stop()
