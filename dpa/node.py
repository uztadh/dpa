from os import wait
import grpc
from concurrent import futures
import time
import dpa.proto.basics_pb2 as basics_pb2
import dpa.proto.basics_pb2_grpc as basics_pb2_grpc
import dpa.logger

import signal


class BasicService(basics_pb2_grpc.BasicServicer):
    def __init__(self, logger=None):
        self.count = 0
        self.log = logger or dpa.logger.null

    def DoPing(self, request, context):
        self.count = self.count + 1
        res = {"status": "Pong", "client_msg": request.client_msg, "count": self.count}
        self.log.info("Basic.DoPing")
        return basics_pb2.PongResponse(**res)


def serve():

    log = dpa.logger.create_logger("server")
    host, port = "localhost", 50051
    address = f"{host}:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    basics_pb2_grpc.add_BasicServicer_to_server(BasicService(logger=log), server)
    server.add_insecure_port(address)

    def signal_handler(signal, frame):
        log.info("stop server")
        server.stop(grace=0)

    signal.signal(signal.SIGINT, signal_handler)
    log.info(f"serving at {address}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
