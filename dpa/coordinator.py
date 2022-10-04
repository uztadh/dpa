from logging import Logger
import random
from queue import PriorityQueue
from typing import Protocol, Mapping, Optional
from enum import IntEnum
import dpa.logger


class CoordinatorCloud(Protocol):
    def add_datastore(self) -> bool:
        ...

    def remove_datastore(self, cloud_ID: int):
        ...

    def shutdown(self):
        ...


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


def main():
    log = dpa.logger.default
    SEED = 42
    r = random.Random(SEED)
    lb = DefaultLoadBalancer(logger=log)

    num_shards = 9
    shards = {sh for sh in range(1, num_shards + 1)}
    shard_loads = {shard: r.randint(1, 10) for shard in shards}

    num_servers = 3
    servers = {s for s in range(1, num_servers + 1)}

    server_list = list(servers)
    current_locations = {sh: r.choice(server_list) for sh in shards}

    updated_locations = lb.balance_load(shards, servers, shard_loads, current_locations)
    log.info("current_locations: {}".format(current_locations))
    log.info("updated_locations: {}".format(updated_locations))


if __name__ == "__main__":
    main()
