
from threading import Thread
import pytest
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from sinaspider.config import CONFIG
from sinaspider.services import ttypes
from sinaspider.scheduler import SchedulerServiceHandler
from sinaspider.services.scheduler_service import Client, Processor

class TestSchedulerService:
    client_transport = None
    client = None
    SCHEDULER_SERVICE_CONFIG = None
    def setup_class(cls):
        """
        Called for each test class.
        """
        cls.SCHEDULER_SERVICE_CONFIG = CONFIG['TEST']['scheduler_service']
        host = cls.SCHEDULER_SERVICE_CONFIG['addr']
        port = cls.SCHEDULER_SERVICE_CONFIG['port']
        transport = TSocket.TSocket(host, port)
        cls.client_transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(cls.client_transport)
        cls.client = Client(protocol)
        cls.client_transport.open()

    def test_register_downloader(self):
        ret = TestSchedulerService.client.register_downloader('test') 
        assert ret == ttypes.RetStatus.SUCCESS

    def teardown_class(cls):
        cls.client_transport.close()
