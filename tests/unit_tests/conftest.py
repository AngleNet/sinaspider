
import logging
import pytest

@pytest.fixture(scope='session', autouse=True)
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    yield None
