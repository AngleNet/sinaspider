#!/usr/bin/env python

import logging
import multiprocessing
from os.path import abspath, join, dirname
import signal
import sys

import sinaspider.config
import sinaspider.downloader_pool
import sinaspider.log
import sinaspider.pipeline
import sinaspider.scheduler
import sinaspider.utils


if __name__ == '__main__':
    pid_dir = dirname(abspath(__file__))
    if len(sys.argv) == 3:
        daemon = None
        cmd = sys.argv[1]
        target = sys.argv[2]
        if target == 'scheduler':
            pid_file = join(pid_dir, target+'.pid')
            daemon = sinaspider.scheduler.SchedulerServerDaemon(pid_file)
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
