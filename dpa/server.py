from os import wait
import grpc
import uuid
import time
from concurrent import futures
import time
from tooz import coordination
import signal

import proto.basics_pb2_grpc
import proto.basics_pb2
import dpa.logger


class Node:
    def __init__(self, node_id, capabilities):
        self.node_id = node_id
        self.capabilities = capabilities


class Cluster:
    def __init__(self, coordinator_addr, node: Node, group_id: str, logger=None):
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
        return [str(m) for m in self.members.keys()]

    def get_leader(self):
        return self.c.get_leader(self.group_id).get()

    def start(self):
        try:
            while True:
                self.c.run_watchers()
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        self.stop()

    def stop(self):
        if self.is_leader:
            self.c.stand_down_group_leader(self.group_id)

        # leave the group
        self.c.leave_group(self.group_id).get()
        self.c.stop()


def run_cluster():
    log = dpa.logger.create_logger("cluster")

    node_id = str(uuid.uuid4())[:8]
    node = Node(node_id, {})
    group_id = "dpa_group"
    log.info(f"node={node_id}")

    zk1_host, zk1_port = "127.0.0.1", 21811
    zk_addr = f"zookeeper://{zk1_host}:{zk1_port}"

    cluster = Cluster(zk_addr, node, group_id, log)
    cluster.start()


class BasicService(proto.basics_pb2_grpc.BasicServicer):
    def __init__(self, logger=None):
        self.count = 0
        self.log = logger or dpa.logger.null

    def Ping(self, request, context):
        self.count = self.count + 1
        res = {"status": "Pong", "client_msg": request.client_msg, "count": self.count}
        self.log.info("Basic.DoPing")
        return proto.basics_pb2.PongResponse(**res)


def serve_grpc():

    log = dpa.logger.create_logger("server")

    host, port = "localhost", 50051
    address = f"{host}:{port}"

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    proto.basics_pb2_grpc.add_BasicServicer_to_server(BasicService(logger=log), server)
    server.add_insecure_port(address)

    def signal_handler(signal, frame):
        log.info("stop server")
        server.stop(grace=0)

    signal.signal(signal.SIGINT, signal_handler)
    log.info(f"serving at {address}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve_grpc()
