
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from sinaspider.config import CONFIG
from sinaspider.services import ttypes
from sinaspider.scheduler import SchedulerServiceHandler
from sinaspider.services.scheduler_service import Client, Processor
import sinaspider.log

logger = logging.getLogger(__name__)

class TestSchedulerService:
    server = None
    client_transport = None
    client = None
    def setup_class(cls):
        sinaspider.log.setup_test_logging()
        host = CONFIG['TEST']['scheduler_service']['addr']
        port = CONFIG['TEST']['scheduler_service']['port']
        transport = TSocket.TSocket(host, port)
        cls.client_transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(cls.client_transport)
        cls.client = Client(protocol)

        handler = SchedulerServiceHandler()
        processor = Processor(handler)
        server_transport = TSocket.TServerSocket(host, port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        cls.server = TServer.TSimpleServer(processor, server_transport, tfactory, pfactory)
        cls.server.serve()
        # You could do one of these for a multithreaded server 
        # server = TServer.TThreadedServer(
        #     processor, transport, tfactory, pfactory)
    def setup_method(self):
        TestSchedulerService.client_transport.open()

    def test_register_downloader(self):
        assert TestSchedulerService.client.request_downloader('test') == ttypes.RetStatus.SUCCESS

    def teardown_method(self):
        TestSchedulerService.client_transport.close()

    def teardown_class(cls):
        cls.client_transport.open()
        cls.client._kill_service()
        cls.client_transport.close()
        