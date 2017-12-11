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


class SchedulerServiceDaemon(sinaspider.utils.Daemon):
    def run(self):
        
        pass