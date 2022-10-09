from logging import Logger
from os import wait
import random
from queue import PriorityQueue
from typing import Protocol, Mapping, Optional, Any, Callable
from enum import IntEnum
from dpa.datastore import DatastoreDescription, ShardDescription
import dpa.logger
from dpa.util import ConsistentHash


class Provisioning(Protocol):
    def add_datastore(self) -> bool:
        ...

    def remove_datastore(self, id_: int):
        ...

    def shutdown(self):
        ...


class DefaultProvisioning(Provisioning):
    def add_datastore(self):
        return True

    def remove_datastore(self, id_):
        pass

    def shutdown(self):
        pass


class ScaleOpt(IntEnum):
    NO_CHANGE = 0
    ADD = 1
    REMOVE = 2


class AutoScaler(Protocol):
    log: Logger

    def autoscale(
        self,
        server_cpu_usage: dict[int, float],
    ) -> ScaleOpt:
        ...


class DefaultAutoScaler(AutoScaler):
    def __init__(self, logger: Optional[Logger] = None):
        if logger is None:
            logger = dpa.logger.null
        self.log = logger
        self.quiscence = 0
        self.quiscence_period = 2
        self.add_server_threshold = 0.7
        self.remove_server_threshold = 0.3

    def autoscale(self, server_cpu_usage: dict[int, float]) -> ScaleOpt:
        if len(server_cpu_usage) == 0:
            return ScaleOpt.NO_CHANGE

        average_cpu_usage = sum(server_cpu_usage.values()) / len(server_cpu_usage)
        self.log.info(f"Average CPU Usage: {average_cpu_usage}")

        # after acting, wait quiescencePeriod cycles before acting again for
        # CPU to rebalance.
        if self.quiscence > 0:
            self.quiscence -= 1
            return ScaleOpt.NO_CHANGE

        self.quiscence = self.quiscence_period

        if average_cpu_usage > self.add_server_threshold:
            # add a server:
            return ScaleOpt.ADD
        elif average_cpu_usage < self.remove_server_threshold:
            # remove a server:
            return ScaleOpt.REMOVE
        else:
            return ScaleOpt.NO_CHANGE


class LoadBalancer(Protocol):
    log: Logger

    def balance_load(
        self,
        shards: set[int],
        servers: set[int],
        shard_loads: Mapping[int, int],
        current_locations: Mapping[int, int],
    ) -> dict[int, int]:
        """
        current_locations: shard_num -> server_num
        """
        ...


class DefaultLoadBalancer(LoadBalancer):
    def __init__(self, logger: Optional[Logger] = None):
        if logger is None:
            logger = dpa.logger.null
        self.log = logger
        self.epsilon_ratio = 5

    def balance_load(
        self,
        shards: set[int],
        servers: set[int],
        shard_loads: dict[int, int],
        current_locations: dict[int, int],
    ) -> dict[int, int]:
        updated_locations = current_locations.copy()
        server_loads: dict[int, int] = {s: 0 for s in servers}
        server_to_shards: dict[int, list[int]] = {s: [] for s in servers}

        for shard in shards:
            shard_load = shard_loads[shard]
            server = current_locations[shard]
            server_loads[server] = server_loads[server] + shard_load
            server_to_shards[server].append(shard)

        server_min_queue = PriorityQueue(len(servers))
        server_max_queue = PriorityQueue(len(servers))
        for server, load in server_loads.items():
            server_min_queue.put((load, server))
            server_max_queue.put((-load, server))

        average_load: float = sum(shard_loads.values()) / len(server_loads)
        epsilon: float = average_load / self.epsilon_ratio

        while not server_max_queue.empty():
            max_load, overloaded_server = server_max_queue.get(block=False)
            max_load = -max_load
            if max_load <= average_load + epsilon:
                break
            while True:
                shards_in_overloaded_server = server_to_shards[overloaded_server]
                if not any(shard_loads[s] > 0 for s in shards_in_overloaded_server):
                    break
                if server_loads[overloaded_server] <= average_load + epsilon:
                    break

                min_load, underloaded_server = server_min_queue.get(block=False)

                shard_loads_for_shards_in_overloaded_server = {
                    shard: shard_loads[shard]
                    for shard in server_to_shards[overloaded_server]
                }

                most_loaded_shard = max(
                    shards_in_overloaded_server,
                    key=lambda k: shard_loads_for_shards_in_overloaded_server[k],
                )
                most_loaded_shardload = shard_loads[most_loaded_shard]
                server_to_shards[overloaded_server].remove(most_loaded_shard)
                if (
                    server_loads[underloaded_server] + most_loaded_shardload
                    <= average_load + epsilon
                ):
                    assert updated_locations[most_loaded_shard] == overloaded_server
                    updated_locations[most_loaded_shard] = underloaded_server
                    server_loads[overloaded_server] = max_load - most_loaded_shardload

                    server_loads[underloaded_server] = min_load + most_loaded_shardload
                    self.log.info(
                        f"Shard {most_loaded_shard} transfered from DS {overloaded_server} to DS {underloaded_server}"
                    )
                    server_min_queue.put(
                        (min_load + most_loaded_shardload, underloaded_server)
                    )
                else:
                    server_min_queue.put((min_load, underloaded_server))

        return updated_locations


class CoordinatorDCS(Protocol):
    log: Logger

    def close(self):
        ...

    def set_datastore_description(self, desc: DatastoreDescription):
        ...

    def get_shard_description(self, shard: int) -> Optional[ShardDescription]:
        ...

    def set_consistent_hash_function(self, constistent_hash: ConsistentHash):
        ...


class DefaultCoordinatorDCS(CoordinatorDCS):
    def __init__(self, logger: Optional[Logger] = None):
        if logger is None:
            logger = dpa.logger.null
        self.log = logger
        self.datastore_descriptions = {}
        self.shard_descriptions = {}
        self.consistent_hash_fn = None

    def close(self):
        pass

    def set_datastore_description(self, descr: DatastoreDescription):
        self.datastore_descriptions[descr.id_] = descr

    def get_shard_description(self, shard: int) -> Optional[ShardDescription]:
        try:
            return self.shard_descriptions[shard]
        except KeyError as e:
            self.log.error(f"get ShardDescription for {shard} error: {e}")
            return None

    def set_consistent_hash_function(self, fn: ConsistentHash):
        self.consistent_hash_fn = fn


class Coordinator:
    log: Logger = dpa.logger.null
    address: str
    dcs: CoordinatorDCS
    load_balancer: LoadBalancer
    autoscaler: AutoScaler
    provisioning: Provisioning
    consistent_hash_lock: Any

    class Opt:
        @staticmethod
        def address(host: str, port: int):
            def opt(c: Coordinator):
                c.address = f"{host}:{port}"

            return opt

        @staticmethod
        def load_balancer(lb: LoadBalancer):
            def opt(c: Coordinator):
                c.load_balancer = lb

            return opt

        @staticmethod
        def autoscaler(as_: AutoScaler):
            def opt(c: Coordinator):
                c.autoscaler = as_

            return opt

        @staticmethod
        def provisioning(p: Provisioning):
            def opt(c: Coordinator):
                c.provisioning = p

            return opt

        @staticmethod
        def logger(logger: Logger):
            def opt(c: Coordinator):
                c.log = logger

            return opt

    def __init__(self, *opts: Callable[["Coordinator"], None]):
        for set_opt in opts:
            set_opt(self)

        try:
            self.load_balancer
        except AttributeError:
            self.log.info("using default load_balancer")
            self.load_balancer = DefaultLoadBalancer()

        try:
            self.autoscaler
        except AttributeError:
            self.log.info("using default autoscaler")
            self.autoscaler = DefaultAutoScaler()

        try:
            self.provisioning
        except AttributeError:
            self.log.info("using default provisioning")
            self.provisioning = DefaultProvisioning()

    def add_replica(self, shard_num: int, replica_ID: int):
        raise NotImplementedError

    def remove_shard(self, shard_num: int, target_ID: int):
        raise NotImplementedError

    def collect_load(self):
        raise NotImplementedError

    def add_datastore(self):
        raise NotImplementedError

    def remove_datastore(self):
        raise NotImplementedError

    def assign_shards(self):
        raise NotImplementedError

    def rebalance_consistent_hash(self, qps_load: dict[int, int]):
        raise NotImplementedError


def main():
    log = dpa.logger.default

    opt = Coordinator.Opt

    coordinator = Coordinator(
        opt.address("localhost", 9000),
        opt.logger(log),
    )


if __name__ == "__main__":
    main()
