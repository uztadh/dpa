from os import wait
import grpc
import uuid
import time
from concurrent import futures
import time
from tooz import coordination
import signal

import proto.cluster_pb2_grpc
import proto.cluster_pb2
import dpa.logger


class NodeMetadata:
    def __init__(self, node_id, capabilities):
        self.node_id = node_id
        self.capabilities = capabilities


class ClusterMetadata:
    def __init__(
        self, coordinator_addr, node: NodeMetadata, group_id: str, logger=None
    ):
        self.log = logger or dpa.logger.null

        # get coordinator (zookeeper)
        self.c = coordination.get_coordinator(coordinator_addr, node.node_id)
        self.c.start(start_heart=True)
        self.group_id = group_id

        # create the group if it does not exist
        try:
            self.c.create_group(group_id).get()
        except coordination.GroupAlreadyExist:
            pass

        # join group
        self.c.join_group(self.group_id, capabilities=node.capabilities).get()

        # get group members
        member_list = self.c.get_members(self.group_id).get()
        self.members = {
            m: cp.get()
            for m, cp in [
                (m, self.c.get_member_capabilities(group_id, m))
                for m in member_list
                if m != b"leader"
            ]
        }

        # watch member changes
        self.c.watch_join_group(group_id, self._handle_group_membership_change)
        self.c.watch_leave_group(group_id, self._handle_group_membership_change)

        # leadership changes
        self.is_leader = False
        self.c.watch_elected_as_leader(group_id, self._handle_leader_election)

    # watch group membership
    def _handle_group_membership_change(self, event):
        if isinstance(event, coordination.MemberJoinedGroup):
            member_id = event.member_id
            capabilities = self.c.get_member_capabilities(
                event.group_id, member_id
            ).get()
            self.members[member_id] = capabilities
            self.log.info(f"member joined: {member_id}")
        elif isinstance(event, coordination.MemberLeftGroup):
            member_id = str(event.member_id)
            try:
                del self.members[member_id]
            except KeyError:
                pass
            self.log.info(f"member left: {member_id}")
        else:
            raise Exception(f"callback does not handle event: {event}")

    def _handle_leader_election(self, event):
        self.is_leader = True
        self.log.info("is leader")

    def get_members(self) -> list[str]:
        self.c.run_watchers()
        return [m.decode("utf-8") for m in self.members.keys()]

    def get_leader(self) -> str:
        self.c.run_watchers()
        leader: bytes = self.c.get_leader(self.group_id).get()
        return leader.decode("utf-8")

    def close(self):
        if self.is_leader:
            self.c.stand_down_group_leader(self.group_id)

        # leave the group
        self.c.leave_group(self.group_id).get()
        self.c.stop()


class ClusterMetadataService(proto.cluster_pb2_grpc.ClusterServicer):
    def __init__(self, cluster: ClusterMetadata, logger=None):
        self.cluster = cluster
        self.log = logger or dpa.logger.null

    def GetLeader(self, request, context):
        leader = self.cluster.get_leader()
        node = {"node_id": leader}
        return proto.cluster_pb2.NodeDetails(**node)

    def GetMembers(self, request, context):
        leader = self.cluster.get_leader()
        members = self.cluster.get_members()
        res = proto.cluster_pb2.MemberDetails(
            leader=leader,
            members=[
                proto.cluster_pb2.NodeDetails(node_id=node_id) for node_id in members
            ],
        )
        return res


def serve():

    # config
    log = dpa.logger.create_logger("server")

    host, port = "localhost", 50051
    address = f"{host}:{port}"

    node_id = str(uuid.uuid4())[:8]
    node = NodeMetadata(node_id, {})
    group_id = "dpa_group"
    log.info(f"node={node_id}")

    zk1_host, zk1_port = "127.0.0.1", 21811
    zk_addr = f"zookeeper://{zk1_host}:{zk1_port}"

    cluster = ClusterMetadata(zk_addr, node, group_id, log)

    # set up grpc
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    proto.cluster_pb2_grpc.add_ClusterServicer_to_server(
        ClusterMetadataService(cluster, logger=log), server
    )
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
