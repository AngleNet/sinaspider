#!/usr/bin/env python

import multiprocessing
from os.path import abspath, join, dirname
import sys

import sinaspider.config
import sinaspider.downloader_pool
import sinaspider.log
import sinaspider.pipeline
import sinaspider.scheduler
import sinaspider.utils


class SinaSpiderDaemon(sinaspider.utils.Daemon):

    def run(self):
        try:
            log_server = None
            scheduler_server = None
            scheduler_client = None
            downloader_server = None
            pipeline_engine = None

            log_queue = multiprocessing.Queue(-1)
            log_server = multiprocessing.Process(target=sinaspider.log.log_listener,
                args=(log_queue,))
            log_server.start()

            if sinaspider.config.SCHEDULER_CONFIG['alive']:
                scheduler_server = sinaspider.scheduler.SchedulerServer(log_queue)
                scheduler_server.start()
            
            scheduler_client = sinaspider.scheduler.SchedulerServiceClient(log_queue)

            pipeline_engine = sinaspider.pipeline.PipelineEngine()
            # TODO: create a downloading completer callback.
            callbacks = list()
            downloader_pool = sinaspider.downloader_pool.DownloaderPool(callbacks, log_queue)
            downloader_server = multiprocessing.Process(target=downloader_pool.start)
            downloader_server.start()

            scheduler_client.run()
        except Exception:
            if log_server.is_alive():
                log_server.stop()
                log_server.join()
            if scheduler_server and scheduler_server.is_alive():
                scheduler_server.stop()
                scheduler_server.join()
            if downloader_server and downloader_server.is_alive():
                downloader_server.stop()
                downloader_server.join()
            if pipeline_engine:
                pipeline_engine.stop()
            if scheduler_client and scheduler_client.is_alive():
                scheduler_client.stop()
                scheduler_client.join()
           

if __name__ == '__main__':
    pid_file = join(dirname(abspath(__file__)), 'sinaspider.pid')
    daemon = SinaSpiderDaemon(pid_file)
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
