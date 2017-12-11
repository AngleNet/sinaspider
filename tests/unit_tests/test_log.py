import logging
import multiprocessing
import pytest

import sinaspider.log


def test_logging():
    logger = logging.getLogger(__name__)
    logger.info('logger tester')
