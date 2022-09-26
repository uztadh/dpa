import grpc

import proto.basics_pb2 as basics_pb2
import proto.basics_pb2_grpc as basics_pb2_grpc


def run():
    host, port = "localhost", 50051
    address = f"{host}:{port}"
    with grpc.insecure_channel(address) as channel:
        stub = basics_pb2_grpc.BasicStub(channel)
        req = basics_pb2.PingRequest(client_msg="Hello from client")
        res = stub.Ping(req)
        print(res)


if __name__ == "__main__":
    run()
