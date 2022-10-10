from logging import Logger
from dataclasses import dataclass
from queue import PriorityQueue
from typing import Protocol, Mapping, Optional
from enum import IntEnum
from dpa.datastore import DatastoreDescription, DatastoreStatus, ShardDescription
import dpa.logger
from dpa.util import ConsistentHash, DefaultConsistentHash


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
    log: Logger = dpa.logger.null
    consistent_hash: ConsistentHash

    def close(self):
        raise NotImplementedError

    def set_datastore_description(self, descr: DatastoreDescription):
        raise NotImplementedError

    def get_datastore_description(self, ds_id: int) -> Optional[DatastoreDescription]:
        raise NotImplementedError

    def get_shard_description(self, shard: int) -> Optional[ShardDescription]:
        raise NotImplementedError

    def get_replica_datastores_for_shard(self, shard_num: int) -> set[int]:
        raise NotImplementedError

    def add_replica_datastore_for_shard(self, replica_ID: int, shard_num: int):
        raise NotImplementedError

    def get_and_increment_datastore_number(self) -> int:
        raise NotImplementedError


class DefaultCoordinatorDCS(CoordinatorDCS):
    # TODO: lock for consistent hash
    # TODO: lock/atomic int for datastore number
    def __init__(
        self, consistent_hash: ConsistentHash, logger: Optional[Logger] = None
    ):
        if logger is not None:
            self.log = logger
        self.datastore_descriptions = {}
        self.shard_descriptions = {}
        self.consistent_hash = consistent_hash
        self.datastore_number = 0
        self.shard_to_replica_datastores: set[int] = set()

    def close(self):
        pass

    def set_datastore_description(self, descr: DatastoreDescription):
        self.datastore_descriptions[descr.id_] = descr

    def get_datastore_description(self, ds_id: int) -> Optional[DatastoreDescription]:
        return self.datastore_descriptions.get(ds_id)

    def get_shard_description(self, shard: int) -> Optional[ShardDescription]:
        try:
            return self.shard_descriptions[shard]
        except KeyError as e:
            self.log.error(f"get ShardDescription for {shard} error: {e}")
            return None

    def get_replica_datastores_for_shard(self, shard_num: int) -> set[int]:
        return self.shard_to_replica_datastores

    def add_replica_datastore_for_shard(self, replica_ID: int, shard_num: int):
        self.shard_to_replica_datastores.add(shard_num)

    def get_and_increment_datastore_number(self) -> int:
        curr = self.datastore_number
        self.datastore_number += 1
        return curr


@dataclass
class Load:
    qps: dict[int, int]
    memory_usage: dict[int, int]
    server_cpu_usage: dict[int, float]


class Coordinator:
    def __init__(
        self,
        dcs: CoordinatorDCS,
        provisioning: Provisioning,
        load_balancer: LoadBalancer,
        auto_scaler: AutoScaler,
        logger: Optional[Logger] = None,
    ):
        if logger is None:
            logger = dpa.logger.null
        self.log = logger
        self.dcs = dcs
        self.provisioning = provisioning
        self.load_balancer = load_balancer
        self.auto_scaler = auto_scaler

    def add_replica_datastore_for_shard(self, shard_num: int, replica_ID: int):
        raise NotImplementedError

        # TODO: hold shardMapLock

        # get replica_ID current status
        replica_description = self.dcs.get_datastore_description(replica_ID)
        if replica_description is None:
            raise Exception(f"Invalid replica ID {replica_ID}, does not exist")
        if replica_description.status == DatastoreStatus.DEAD:
            raise Exception(f"Datastore {replica_ID} status: dead")

        # get primary datastore for given shard
        # TODO: learn how primary datastore for a given shard are assigned
        primary_datastore_id = self.dcs.consistent_hash.get_random_bucket(shard_num)

        if replica_ID == primary_datastore_id:
            raise Exception(
                f"Replica datastore {replica_ID} is already set as primary datastore for shard {shard_num}"
            )

        # get all replica datastores for given shard
        curr_replicas = self.dcs.get_replica_datastores_for_shard(shard_num)

        # if replica already in set, error out, or maybe make this a noop
        if replica_ID in curr_replicas:
            raise Exception(
                f"Already set datastore {replica_ID} as replica for shard {shard}"
            )

        # send message to replica datastore to load shard
        # add replica_ID to shard's replica list
        # if fails, remove replica from replica list
        self.dcs.add_replica_datastore_for_shard(replica_ID, shard_num)

    def remove_replica_datastore_for_shard(self, shard_num: int, replica_ID: int):
        raise NotImplementedError

        # TODO: hold shardmap lock

        # get primary datastore

        # if replica is primary datastore, error out, invalid operation

        # get replica list

        # if replica not in replica list for given shard, error out, invalid
        # operation

        # remove replica from set of replicas being tracked by DCS

        # send remove shard message to the replica datastore

        # notify primary datastore that replica no longer holds a replica for
        # the given shard

    def add_datastore(self):
        raise NotImplementedError

        # add datastore via provisioning

    def remove_datastore(self):
        raise NotImplementedError

        # get list of datastores

        # build list of removable datastore IDs as follows:
        #   filter out all non-ALIVE datastores, i.e. keep alive ones only
        #   filter out all datastores not in dsIDToCloudID, this stores mappings
        #       from datastore IDs to the cloud IDs (cloudIDs are uniquely
        #       assigned by the autoscaler to respective datastore IDs)

        # get count of active datastores, will be used to pick random datastore
        # to remove

        # if number of active datastores is less than or equal to 1 OR number of
        # removable datastores is 0, don't remove any datastores and return
        # immediately

        # pick a random datastore from the removable set
        # get its respective cloud/provisioning ID
        # remove from consistent_hash
        # assign shards
        # set its status to DEAD
        # remove from cloud/provisioning

    def assign_shards(self):
        raise NotImplementedError

        # get list of datastores from the consistent_hash

        # create new consistent_hash

        # build list of all shards from table info map
        # tabe_info_map has the following signature: dict[string, TableInfo]

        # send a reshuffle message to all datastores with the new
        # consistent_hash plus shard list. ie, invoke execute_reshuffle_add

        # remove unassigned shards via invoking execute_reshuffle_remove, also
        # uses tne new consistent_hash plus shard list

    def collect_load(self) -> Load:

        # load across all datastores
        load = Load(
            qps=dict(),
            memory_usage=dict(),
            server_cpu_usage=dict(),
        )

        # for each shard, keep total count
        shard_count_map: dict[int, int] = dict()

        # for each datastore whose status is ALIVE
        #   get shard usage. shard usage has the following fields:
        #       - ds_ID int32
        #       - shard_qps: map shard->queries per second
        #       - shard_memory_usage: map shard->memory usage
        #       - server_cpu_usage: float
        #   add/merge datastore's qps per shard to total load's
        #   add/merge datastore's memory usage per shard to total load's
        #   add datastore's server_cpu_usage to load

        # use shard count to normalize the memory usage per shard, i.e.
        # for each shard's memory usage, divide by shard's count/replica

        raise NotImplementedError
        return load

    def rebalance_shards(self, qps_load: dict[int, int]):
        # raise NotImplementedError
        shards = set(qps_load.keys())
        servers: set[int] = set()  # TODO retrieve from consistent_hash ?
        current_locations: dict[
            int, int
        ] = dict()  # TODO retrieve from consistent_hash ?
        updated_locations = self.load_balancer.balance_load(
            shards,
            servers,
            qps_load,
            current_locations,
        )
        for shard_num, new_location in updated_locations.items():
            # if new_location != prev_location:
            #   put in reassignment map of consistent_hash
            pass

    def balance_load_daemon(self):
        raise NotImplementedError
        # runs as a daemon/asynchronously

        # TODO: figure out how to use memory usage for load balancing

        # in a loop:
        # collect load
        loop = False
        while loop:
            load = self.collect_load()
            if len(load.qps) > 0:
                self.rebalance_shards(load.qps)
                self.assign_shards()
                if self.provisioning != None:
                    action = self.auto_scaler.autoscale(load.server_cpu_usage)
                    if action == ScaleOpt.ADD:
                        self.add_datastore()
                    elif action == ScaleOpt.REMOVE:
                        self.remove_datastore()


def main():
    log = dpa.logger.default

    coordinator = Coordinator(
        dcs=DefaultCoordinatorDCS(consistent_hash=DefaultConsistentHash(), logger=log),
        provisioning=DefaultProvisioning(),
        load_balancer=DefaultLoadBalancer(logger=log),
        auto_scaler=DefaultAutoScaler(logger=log),
        logger=log,
    )


if __name__ == "__main__":
    main()
