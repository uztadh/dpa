import grpc


import proto.cluster_pb2
import proto.cluster_pb2_grpc
import google.protobuf.empty_pb2 as empty


def run():
    host, port = "localhost", 50051
    address = f"{host}:{port}"
    with grpc.insecure_channel(address) as channel:
        stub = proto.cluster_pb2_grpc.ClusterStub(channel)

        # get members
        member_details = stub.GetMembers(empty.Empty())
        leader = member_details.leader
        print("leader:", leader)
        for m in member_details.members:
            print(m)


if __name__ == "__main__":
    run()
