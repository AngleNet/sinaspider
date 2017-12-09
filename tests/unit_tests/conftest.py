
import logging
import multiprocessing
import pytest
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


import sinaspider.log
from sinaspider.scheduler import SchedulerServiceHandler
from sinaspider.services.scheduler_service import Processor, Client

@pytest.fixture(scope='session', autouse=True)
def init():
    """
    Initialize logging for testing. Called before all test starts.

    See https://docs.pytest.org/en/latest/fixture.html
    """
    queue = multiprocessing.Queue(-1)
    log_process = multiprocessing.Process(target=sinaspider.log.log_listener, 
                                      args=(queue,))
    log_process.start()
    sinaspider.log.configure_logger(queue)

    SCHEDULER_SERVICE_CONFIG = sinaspider.log.CONFIG['TEST']['scheduler_service']
    host = SCHEDULER_SERVICE_CONFIG['addr']
    port = SCHEDULER_SERVICE_CONFIG['port']
    handler = SchedulerServiceHandler()
    processor = Processor(handler)
    server_transport = TSocket.TServerSocket(host, port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    tserver = TServer.TSimpleServer(processor, server_transport, tfactory, pfactory)
    scheduler_process = multiprocessing.Process(target=tserver.serve)
    scheduler_process.start()
    yield None
    # Tear down
    queue.put_nowait(None)
    log_process.join()
    scheduler_process.terminate()
    scheduler_process.join()
    